//! Background helper-share tracking.
//!
//! A vote run confirms only the round's immediate share in the foreground.
//! The SDK's tracking driver confirms the rest, reconciles outcome-unknown
//! deliveries, and replenishes lost shares until vote end. This runs it while
//! the app is open: after each cast, and again at launch for every stored
//! round that still has unconfirmed shares. Each run takes its helper fleet
//! from the pinned, authenticated config, never from a stored snapshot, and
//! keeps it current while it runs.

use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};

use reqwest::Client;
use tokio::sync::Notify;
use zcash_voting::{
    config::ResolvedVotingConfig, round::VotingDb, ChainSubmissionControl, HelperClient,
    HelperHealth, ShareTrackingDriver, ShareTrackingEvent, ShareTrackingHostContext,
    ShareTrackingHostSourceBridge, ShareTrackingQuiescence, ShareTrackingReporterBridge,
};

use crate::{
    chain::{self, ConfigFailure, VotingTransport},
    model::{Profile, RoundSnapshot},
    storage::{open_db, profile_paths, read_manifest, wallet_id, ProfilePaths},
    voter::{unix_seconds, vote_end_time},
};

/// Pause before a round's tracking starts again after the SDK gave up on a
/// run of failed passes, or after its config could not be resolved.
///
/// This is the SDK's own wait after a failed pass. A longer pause could sleep
/// through the end of a round whose network comes back in its last minutes.
const RETRY_DELAY: Duration = Duration::from_secs(15);

/// How often a running round re-reads its helper fleet from the pinned config.
const FLEET_REFRESH_INTERVAL: Duration = Duration::from_secs(30 * 60);

/// What a run's task and [`ShareTracking::start`] decide together, under one
/// lock, so a start can never fall between a run's last pass and its exit.
#[derive(Default)]
struct RunState {
    finished: bool,
    /// A start that arrived while the run was active. The run may already have
    /// read the share rows that start was called for, so it takes one more
    /// pass before it finishes.
    rerun: bool,
    /// The SDK is waiting between passes, so a start can cut the wait short
    /// without interrupting a pass.
    waiting: bool,
}

struct Run {
    control: ChainSubmissionControl,
    /// Ends a retry pause early, so stopping or rerunning a run never waits it
    /// out.
    wake: Arc<Notify>,
    state: Arc<Mutex<RunState>>,
    task: tauri::async_runtime::JoinHandle<()>,
}

/// Tracking runs keyed by profile slug and round id.
#[derive(Default)]
pub struct ShareTracking {
    runs: Mutex<HashMap<(&'static str, String), Run>>,
}

impl ShareTracking {
    /// Starts tracking `round`'s helper shares, or has an active run take
    /// another pass.
    ///
    /// `fleet` is the round's helper list when the caller has just taken it
    /// from the pinned config, so a new run can start without a lookup.
    pub fn start(
        &self,
        client: Client,
        transport: Arc<VotingTransport>,
        paths: ProfilePaths,
        profile: Profile,
        round: RoundSnapshot,
        fleet: Option<Vec<String>>,
    ) {
        if profile.is_demo() {
            return;
        }
        let key = (profile.slug(), round.round_id.clone());
        let mut runs = self.runs.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(run) = runs.get(&key) {
            let mut state = run.state.lock().unwrap_or_else(PoisonError::into_inner);
            if !state.finished {
                state.rerun = true;
                run.wake.notify_one();
                // Polls the new shares now instead of when the wait ends. A
                // pass in progress finishes first: its wait is cut short as
                // soon as it starts.
                if state.waiting {
                    end_wait(&run.control);
                }
                return;
            }
        }
        let control = ChainSubmissionControl::new(0);
        let wake = Arc::new(Notify::new());
        let state = Arc::new(Mutex::new(RunState::default()));
        let task = tauri::async_runtime::spawn(track_round(
            client,
            transport,
            paths,
            profile,
            round,
            fleet,
            control.clone(),
            Arc::clone(&wake),
            Arc::clone(&state),
        ));
        runs.insert(
            key,
            Run {
                control,
                wake,
                state,
                task,
            },
        );
    }

    /// Starts tracking every stored round in `profiles` that still has
    /// unconfirmed helper shares.
    ///
    /// Best effort: a profile whose state cannot be read is skipped, and its
    /// rounds resume on the next cast instead.
    pub fn resume(
        &self,
        client: &Client,
        transport: &Arc<VotingTransport>,
        app_data_dir: &Path,
        profiles: &[Profile],
    ) {
        for &profile in profiles {
            let Ok(paths) = profile_paths(app_data_dir, profile) else {
                continue;
            };
            if !paths.database.exists() {
                continue;
            }
            let Ok(manifest) = read_manifest(&paths, profile) else {
                continue;
            };
            let pending = open_db(&paths, profile).and_then(|db| {
                zcash_voting::share::pending_rounds_for_accounts(&db, &[&wallet_id(profile)])
                    .map_err(|error| error.to_string())
            });
            let Ok(pending) = pending else {
                continue;
            };
            for round in pending {
                if let Some(stored) = manifest.rounds.get(&round.round_id) {
                    self.start(
                        client.clone(),
                        Arc::clone(transport),
                        paths.clone(),
                        profile,
                        stored.snapshot.clone(),
                        None,
                    );
                }
            }
        }
    }

    /// Cancels every run for `profile`, or for every profile, and waits until
    /// each one has released its voting database.
    pub async fn stop(&self, profile: Option<Profile>) {
        let stopped = {
            let mut runs = self.runs.lock().unwrap_or_else(PoisonError::into_inner);
            let keys = runs
                .keys()
                .filter(|(slug, _)| profile.is_none_or(|profile| *slug == profile.slug()))
                .cloned()
                .collect::<Vec<_>>();
            keys.into_iter()
                .filter_map(|key| runs.remove(&key))
                .collect::<Vec<_>>()
        };
        for run in &stopped {
            run.control.cancel();
            run.wake.notify_one();
        }
        for run in stopped {
            let _ = run.task.await;
        }
    }
}

/// Tracks one round until its shares settle, it ends, or the run is stopped.
///
/// The helper fleet always comes from the pinned, authenticated config,
/// because the stored snapshot's fleet can be stale or come from a restored
/// backup, and the config can replace a helper mid-round. It is resolved
/// before the first run, again after a failing run or once it is older than
/// the refresh interval, and refreshed while a run is in progress. The last
/// authenticated fleet is kept across runs, so a rerun never waits on a
/// lookup and a config outage does not stop tracking that already has
/// helpers. The round's timing stays the stored one, which the app validated
/// before casting. A server's round response could otherwise drop the vote
/// end, and with it the SDK's overdue resubmission.
///
/// The SDK stops a run after a long streak of failed passes, about an hour at
/// its defaults, which a laptop left open offline can hit. Nothing else would
/// restart it before the next launch, so a failing run starts again after a
/// pause for as long as the round is open.
#[allow(clippy::too_many_arguments)]
async fn track_round(
    client: Client,
    transport: Arc<VotingTransport>,
    paths: ProfilePaths,
    profile: Profile,
    round: RoundSnapshot,
    authenticated_fleet: Option<Vec<String>>,
    control: ChainSubmissionControl,
    wake: Arc<Notify>,
    state: Arc<Mutex<RunState>>,
) {
    let mut fleet = authenticated_fleet.map(|current| (current, Instant::now()));
    let mut failing = false;
    while !control.is_cancelled() {
        {
            let mut run_state = state.lock().unwrap_or_else(PoisonError::into_inner);
            run_state.rerun = false;
            run_state.waiting = false;
        }
        let stale = fleet
            .as_ref()
            .is_none_or(|(_, resolved)| failing || resolved.elapsed() >= FLEET_REFRESH_INTERVAL);
        if stale {
            match resolve_fleet(&client, profile, &round.round_id, &control, &wake).await {
                _ if control.is_cancelled() => break,
                FleetLookup::Authenticated(current) => fleet = Some((current, Instant::now())),
                FleetLookup::NotAuthenticated | FleetLookup::Unsupported => fleet = None,
                FleetLookup::Failed => {}
            }
        }
        let current = fleet.as_ref().map(|(current, _)| current.clone());
        failing = match current {
            Some(current) => {
                let shared = Mutex::new(current);
                let outcome = match open_db(&paths, profile) {
                    Ok(db) => tokio::select! {
                        quiescence = run_tracking(&db, &transport, &round, &shared, &control, &state) => {
                            Some(matches!(quiescence, ShareTrackingQuiescence::Failing { .. }))
                        }
                        () = refresh_fleet(&client, profile, &round.round_id, &shared) => None,
                    },
                    Err(_) => Some(true),
                };
                match outcome {
                    Some(failing) => {
                        // Keep what a refresh during the run learned.
                        if let Some((current, _)) = fleet.as_mut() {
                            *current = shared.into_inner().unwrap_or_else(PoisonError::into_inner);
                        }
                        failing
                    }
                    // The config stopped vouching for the round mid-run.
                    None => {
                        fleet = None;
                        true
                    }
                }
            }
            None => true,
        };
        let ended = vote_end_time(&round)
            .is_some_and(|vote_end| unix_seconds().unwrap_or_default() >= vote_end);
        if ended {
            break;
        }
        if !failing {
            let mut run_state = state.lock().unwrap_or_else(PoisonError::into_inner);
            if !run_state.rerun {
                run_state.finished = true;
                return;
            }
            continue;
        }
        let _ = tokio::time::timeout(RETRY_DELAY, wake.notified()).await;
    }
    state
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .finished = true;
}

/// What one read of the pinned config says about a round's helper fleet.
enum FleetLookup {
    Authenticated(Vec<String>),
    /// The config resolved but no longer authenticates the round. Its helpers
    /// may never have served this round, so the last fleet is dropped.
    NotAuthenticated,
    /// The config advertises a protocol version this build does not support.
    /// The SDK asks wallets to stop using cached voting state until a
    /// supported config returns, so the last fleet is dropped here too.
    Unsupported,
    /// The config could not be read. The last fleet is still the latest
    /// authenticated one, so it stays in use.
    Failed,
}

/// Reads the round's helper fleet from the pinned config.
async fn lookup_fleet(client: &Client, profile: Profile, round_id: &str) -> FleetLookup {
    match chain::resolve_environment(client, profile).await {
        Ok(environment) => round_fleet(&environment, round_id)
            .map_or(FleetLookup::NotAuthenticated, FleetLookup::Authenticated),
        Err(ConfigFailure::Unsupported(_)) => FleetLookup::Unsupported,
        Err(ConfigFailure::Unavailable(_)) => FleetLookup::Failed,
    }
}

/// [`lookup_fleet`] that only a stop abandons. A rerun request waits for it,
/// because the run that follows reads every share that arrived meanwhile.
async fn resolve_fleet(
    client: &Client,
    profile: Profile,
    round_id: &str,
    control: &ChainSubmissionControl,
    wake: &Notify,
) -> FleetLookup {
    let lookup = lookup_fleet(client, profile, round_id);
    tokio::pin!(lookup);
    loop {
        tokio::select! {
            fleet = &mut lookup => return fleet,
            () = wake.notified() => {
                if control.is_cancelled() {
                    return FleetLookup::Failed;
                }
            }
        }
    }
}

/// Runs the SDK's tracking driver once and returns why it stopped.
///
/// Each pass reads the latest `fleet`, and `state` records when the driver
/// waits between passes. The driver records everything it learns durably,
/// and the UI reads progress from that, so the rest of the report is not
/// needed here.
async fn run_tracking(
    db: &VotingDb,
    transport: &Arc<VotingTransport>,
    round: &RoundSnapshot,
    fleet: &Mutex<Vec<String>>,
    control: &ChainSubmissionControl,
    state: &Mutex<RunState>,
) -> ShareTrackingQuiescence {
    let client = HelperClient::new(transport.clone(), HelperHealth::default());
    let vote_end_time_seconds = vote_end_time(round);
    let host = ShareTrackingHostSourceBridge::new(move || ShareTrackingHostContext {
        configured_helper_urls: fleet.lock().unwrap_or_else(PoisonError::into_inner).clone(),
        now_seconds: unix_seconds().unwrap_or_default(),
        vote_end_time_seconds,
    });
    let reporter = ShareTrackingReporterBridge::new(|event| {
        let mut run_state = state.lock().unwrap_or_else(PoisonError::into_inner);
        match event {
            ShareTrackingEvent::PassStarted { .. } => run_state.waiting = false,
            // A start during the pass that just ended is served now.
            ShareTrackingEvent::AwaitingNextPass { .. } if run_state.rerun => end_wait(control),
            ShareTrackingEvent::AwaitingNextPass { .. } => run_state.waiting = true,
            _ => {}
        }
    });
    ShareTrackingDriver::new(db, &client, &round.round_id)
        .run(&host, control, &reporter)
        .await
        .quiescence
}

/// Ends the SDK's wait between passes by moving the run to a new operation
/// epoch. The run then stops, and its task starts the next one at once.
fn end_wait(control: &ChainSubmissionControl) {
    control.set_operation_epoch(control.operation_epoch().wrapping_add(1));
}

/// Re-reads the helper fleet from the pinned config at a fixed interval, for as
/// long as the run it serves is polled. A read that fails keeps the last fleet.
/// Returns once the config no longer authenticates the round or this build no
/// longer supports it, so the run stops (see [`FleetLookup`]).
async fn refresh_fleet(
    client: &Client,
    profile: Profile,
    round_id: &str,
    fleet: &Mutex<Vec<String>>,
) {
    loop {
        tokio::time::sleep(FLEET_REFRESH_INTERVAL).await;
        match lookup_fleet(client, profile, round_id).await {
            FleetLookup::Authenticated(current) => {
                *fleet.lock().unwrap_or_else(PoisonError::into_inner) = current;
            }
            FleetLookup::NotAuthenticated | FleetLookup::Unsupported => return,
            FleetLookup::Failed => {}
        }
    }
}

/// The helper URLs the resolved config lists, in its order, or `None` when it
/// no longer authenticates `round_id`. A config that moved to another round
/// or chain can list helpers that never served this one.
fn round_fleet(environment: &ResolvedVotingConfig, round_id: &str) -> Option<Vec<String>> {
    environment
        .authenticated_rounds
        .iter()
        .any(|round| round.round_id == round_id)
        .then(|| {
            environment
                .vote_servers
                .iter()
                .map(|endpoint| endpoint.url.clone())
                .collect()
        })
}
