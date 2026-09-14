use crate::{
    model::{
        CastVotesResult, ImportResult, Profile, RoundSnapshot, StoredRound, TargetResult,
        VoteChoiceInput, VoteProgressEvent, VoteTransactionResult,
    },
    storage::{load_hotkey, open_db, read_manifest, store_hotkey, write_manifest, ProfilePaths},
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use reqwest::Client;
use rusqlite::named_params;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tauri::{AppHandle, Emitter};
use zcash_voting::{
    delegation_capability::{
        import_delegation_capability, ImportDelegationCapabilityParams,
        MAX_DELEGATION_CAPABILITY_JSON_BYTES,
    },
    hotkey::generate_random_voting_hotkey,
    prelude::*,
    round_drive::{RoundDriveReporterBridge, RoundHostSourceBridge},
    wire::VotingHotkeyTargetV1,
    HyperTransport, VotingRoundParams,
};
use zeroize::Zeroizing;

pub fn target_json(
    hotkey: &VotingHotkey,
    chain_id: &str,
    params: &VotingRoundParams,
) -> Result<String, String> {
    let target = hotkey.delegation_target();
    let wire = VotingHotkeyTargetV1 {
        format_version: 1,
        vote_chain_id: chain_id.to_string(),
        network: match target.network() {
            zcash_voting::Network::Mainnet => "mainnet",
            zcash_voting::Network::Testnet => "testnet",
            zcash_voting::Network::Regtest => "regtest",
        }
        .to_string(),
        vote_round_id: params.vote_round_id.clone(),
        address_index: target.address_index(),
        raw_orchard_address: BASE64_STANDARD.encode(target.raw_orchard_address()),
    };
    wire.validate_for(chain_id, target.network(), params)
        .map_err(|error| format!("validate voting target failed: {error}"))?;
    wire.to_json()
        .map_err(|error| format!("serialize voting target failed: {error}"))
}

pub fn generate_target(
    paths: &ProfilePaths,
    profile: Profile,
    round: RoundSnapshot,
) -> Result<TargetResult, String> {
    require_round_context(profile, &round)?;
    let mut manifest = read_manifest(paths, profile)?;
    if let Some(stored) = manifest.rounds.get_mut(&round.round_id) {
        validate_stored_round(stored, &round)?;
        let hotkey = load_hotkey(profile, &round.round_id).map_err(|error| {
            format!(
                "the target exists but its voting hotkey is unavailable; restore a backup instead of generating a replacement: {error}"
            )
        })?;
        let regenerated = target_json(&hotkey, &round.chain_id, &round.params)?;
        if regenerated != stored.target_json {
            return Err("stored voting hotkey does not match the exported target".to_string());
        }
        stored.snapshot = round;
        write_manifest(paths, &manifest)?;
        return Ok(TargetResult {
            target_json: regenerated,
            reused: true,
        });
    }
    let now = unix_seconds()?;
    if !round_accepts_setup(profile, &round, now) {
        if !profile.is_demo() && round.vote_end_time <= now {
            return Err(
                "the voting deadline has passed and this round no longer accepts new voting targets"
                    .to_string(),
            );
        }
        return Err(format!(
            "round is {} and no longer accepts new voting targets",
            round.status_label.to_lowercase()
        ));
    }

    let hotkey = generate_random_voting_hotkey(profile.network())
        .map_err(|error| format!("generate voting hotkey failed: {error}"))?;
    let target = target_json(&hotkey, &round.chain_id, &round.params)?;
    store_hotkey(profile, &round.round_id, hotkey.stored_secret())?;
    manifest.rounds.insert(
        round.round_id.clone(),
        StoredRound {
            snapshot: round.clone(),
            target_json: target.clone(),
            capability_digest: None,
        },
    );
    if let Err(error) = write_manifest(paths, &manifest) {
        let _ = crate::storage::delete_hotkey(profile, &round.round_id);
        return Err(error);
    }
    Ok(TargetResult {
        target_json: target,
        reused: false,
    })
}

pub async fn import_capability(
    client: &Client,
    paths: &ProfilePaths,
    profile: Profile,
    round: RoundSnapshot,
    capability_bytes: Vec<u8>,
) -> Result<ImportResult, String> {
    require_round_context(profile, &round)?;
    if capability_bytes.is_empty() || capability_bytes.len() > MAX_DELEGATION_CAPABILITY_JSON_BYTES
    {
        return Err(format!(
            "custody payload must contain 1..={MAX_DELEGATION_CAPABILITY_JSON_BYTES} bytes"
        ));
    }
    let mut manifest = read_manifest(paths, profile)?;
    let stored = manifest.rounds.get(&round.round_id).ok_or_else(|| {
        "generate and send the voting target before importing a custody payload".to_string()
    })?;
    validate_stored_round(stored, &round)?;
    let hotkey = load_hotkey(profile, &round.round_id)?;
    let db = Arc::new(open_db(paths, profile)?);
    let digest = import_delegation_capability(
        &db,
        &capability_bytes,
        ImportDelegationCapabilityParams {
            voting_hotkey: &hotkey,
            expected_chain_id: &round.chain_id,
            expected_network: profile.network(),
            expected_round_params: &round.params,
            session_json: None,
        },
    )
    .map_err(|error| format!("import custody payload failed: {error}"))?
    .to_hex();

    let stored = manifest
        .rounds
        .get_mut(&round.round_id)
        .expect("stored round was checked above");
    stored.snapshot = round.clone();
    stored.capability_digest = Some(digest.clone());
    persist_capability_manifest(paths, &manifest)?;

    if profile.is_demo() {
        // The demo keeps confirmation as an explicit customer-visible step.
    } else {
        refresh_delegations_once(client, &db, &round).await?;
    }
    import_result(&db, &round.round_id, digest)
}

fn persist_capability_manifest(
    paths: &ProfilePaths,
    manifest: &crate::model::ProfileManifest,
) -> Result<(), String> {
    // An imported capability represents an already-broadcast delegation. Its
    // recovery state must survive even if the separate UI manifest cannot be written.
    write_manifest(paths,manifest).map_err(|error|format!("{error}; custody recovery state was preserved; retry the same payload to finish local recovery"))
}

pub async fn refresh_delegations(
    client: &Client,
    paths: &ProfilePaths,
    profile: Profile,
    round: &RoundSnapshot,
) -> Result<ImportResult, String> {
    let manifest = read_manifest(paths, profile)?;
    let stored = manifest
        .rounds
        .get(&round.round_id)
        .ok_or_else(|| "this round has no local voting target".to_string())?;
    validate_stored_round(stored, round)?;
    let digest = stored
        .capability_digest
        .clone()
        .ok_or_else(|| "import the custody payload before checking confirmations".to_string())?;
    let db = Arc::new(open_db(paths, profile)?);
    if profile.is_demo() {
        crate::demo::confirm_delegations(Arc::clone(&db), round).await?;
    } else {
        refresh_delegations_once(client, &db, round).await?;
    }
    import_result(&db, &round.round_id, digest)
}

async fn refresh_delegations_once(
    _client: &Client,
    db: &Arc<VotingDb>,
    round: &RoundSnapshot,
) -> Result<(), String> {
    if round.profile.is_demo() {
        return crate::demo::confirm_delegations(Arc::clone(db), round).await;
    }
    let client = ChainSubmissionClient::new(Arc::clone(db), chain_config(round))
        .map_err(|e| format!("create delegation client failed: {e}"))?;
    let control = ChainSubmissionControl::new(0);
    for (bundle_index, _, position) in delegation_rows(db, &round.round_id)? {
        if position.is_some() {
            continue;
        }
        let result = client
            .advance_imported_delegation(
                AdvanceImportedDelegation {
                    vote_round_id: round_bytes(round)?,
                    bundle_index,
                },
                &control,
            )
            .await
            .map_err(|e| format!("confirm custody delegation failed: {e}"))?;
        match result {
            ChainSubmissionResult::Rejected(diagnostic)
            | ChainSubmissionResult::SubmittedWithoutHash(diagnostic) => {
                return Err(format!("delegation could not be confirmed: {diagnostic:?}"))
            }
            _ => (),
        }
    }
    Ok(())
}

pub fn round_bytes(round: &RoundSnapshot) -> Result<[u8; 32], String> {
    hex::decode(&round.round_id)
        .map_err(|e| e.to_string())?
        .try_into()
        .map_err(|_| "round id must contain 32 bytes".to_string())
}
pub fn chain_config(round: &RoundSnapshot) -> ChainSubmissionClientConfig {
    ChainSubmissionClientConfig::for_network(round.profile.network(), server_urls(round))
        .with_vote_chain_id(&round.chain_id)
}
pub fn server_urls(round: &RoundSnapshot) -> Vec<String> {
    round
        .vote_servers
        .iter()
        .map(|server| server.url.clone())
        .collect()
}
pub fn helper_client() -> HelperClient {
    HelperClient::new(Arc::new(HyperTransport::new()), HelperHealth::default())
}

fn import_result(db: &VotingDb, round_id: &str, digest: String) -> Result<ImportResult, String> {
    let rows = delegation_rows(db, round_id)?;
    let confirmed_bundle_count = rows
        .iter()
        .filter(|(_, _, position)| position.is_some())
        .count();
    Ok(ImportResult {
        digest,
        bundle_count: u32::try_from(rows.len())
            .map_err(|_| "bundle count exceeds u32".to_string())?,
        confirmed_bundle_count: u32::try_from(confirmed_bundle_count)
            .map_err(|_| "confirmed bundle count exceeds u32".to_string())?,
        pending_transaction_hashes: rows
            .into_iter()
            .filter_map(|(_, hash, position)| position.is_none().then_some(hash))
            .collect(),
    })
}

fn delegation_rows(
    db: &VotingDb,
    round_id: &str,
) -> Result<Vec<(u32, String, Option<u32>)>, String> {
    let wallet = db.wallet_id();
    let conn = db.conn();
    let mut statement = conn
        .prepare(
            "SELECT bundle_index, delegation_tx_hash, van_leaf_position
             FROM bundles
             WHERE round_id = :round_id AND wallet_id = :wallet_id
             ORDER BY bundle_index",
        )
        .map_err(|error| format!("prepare delegation recovery query failed: {error}"))?;
    let rows = statement
        .query_map(
            named_params! { ":round_id": round_id, ":wallet_id": wallet },
            |row| {
                Ok((
                    row.get::<_, u32>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<u32>>(2)?,
                ))
            },
        )
        .map_err(|error| format!("query delegation recovery state failed: {error}"))?;
    rows.map(|row| {
        let (index, hash, position) =
            row.map_err(|error| format!("decode delegation recovery row failed: {error}"))?;
        Ok((
            index,
            hash.ok_or_else(|| format!("delegation bundle {index} has no transaction hash"))?,
            position,
        ))
    })
    .collect()
}

pub fn demo_capability(_paths: &ProfilePaths, round_id: &str) -> Result<String, String> {
    let round = crate::demo::round_snapshot();
    if round.round_id != round_id {
        return Err("unknown local demo round".to_string());
    }
    let hotkey = load_hotkey(Profile::Demo, round_id)?;
    crate::demo::capability_json(&hotkey)
}

pub async fn cast_votes(
    app: AppHandle,
    _client: Client,
    paths: ProfilePaths,
    profile: Profile,
    round: RoundSnapshot,
    choices: Vec<VoteChoiceInput>,
) -> Result<CastVotesResult, String> {
    require_round_context(profile, &round)?;
    validate_choices(&round, &choices)?;
    let manifest = read_manifest(&paths, profile)?;
    let stored = manifest
        .rounds
        .get(&round.round_id)
        .ok_or("generate a voting target before voting")?;
    validate_stored_round(stored, &round)?;
    if stored.capability_digest.is_none() {
        return Err("import the custody payload before voting".into());
    }
    let hotkey = load_hotkey(profile, &round.round_id)?;
    let db = Arc::new(open_db(&paths, profile)?);
    // Keep the application's original no-revote contract, including signed work
    // that has not reached the chain yet. The SDK owns all subsequent recovery.
    for vote in crate::storage::vote_records(&paths, profile, &round.round_id)? {
        if choices
            .iter()
            .any(|c| c.proposal_id == vote.proposal_id && c.choice != vote.choice)
        {
            return Err(format!(
                "proposal {} already has a persisted vote with a different choice",
                vote.proposal_id
            ));
        }
    }
    let report = if profile.is_demo() {
        let (executor, tree) = crate::demo::executor(Arc::clone(&db), &round, &hotkey)?;
        drive_round(&executor, &round, &choices, &hotkey, Some(tree), Some(&app)).await?
    } else {
        let executor = RoundExecutor::with_transport(
            Arc::clone(&db),
            VotingWindowTransport {
                inner: HyperTransport::new(),
                vote_end_time: round.vote_end_time,
            },
            chain_config(&round),
            helper_client(),
        )
        .map_err(|e| e.to_string())?
        .with_binding(round_binding(&round, &hotkey))
        .map_err(|e| e.to_string())?;
        drive_round(&executor, &round, &choices, &hotkey, None, Some(&app)).await?
    };
    let (outcome, message) = describe_run(&report);
    let share_rows = zcash_voting::share::list(&db, &round.round_id).map_err(|e| e.to_string())?;
    let transactions = crate::storage::vote_records(&paths, profile, &round.round_id)?
        .into_iter()
        .filter_map(|vote| {
            Some(VoteTransactionResult {
                bundle_index: vote.bundle_index,
                proposal_id: vote.proposal_id,
                tx_hash: vote.tx_hash?,
                vc_tree_position: vote.vc_tree_position?,
                shares_submitted: share_rows
                    .iter()
                    .filter(|s| {
                        s.bundle_index == vote.bundle_index
                            && s.proposal_id == vote.proposal_id
                            && (s.confirmed
                                || (!s.sent_to_urls.is_empty()
                                    && s.sent_to_urls.len() >= s.target_count.max(1) as usize))
                    })
                    .count() as u32,
            })
        })
        .collect();
    let progress = crate::storage::round_progress(
        &paths,
        profile,
        &round.round_id,
        true,
        true,
        &round.vote_servers,
    )?;
    emit_progress(
        &app,
        &round.round_id,
        &outcome,
        None,
        None,
        None,
        message.clone(),
    );
    Ok(CastVotesResult {
        demo: profile.is_demo(),
        proof_count: progress.vote_count,
        transactions,
        outcome,
        message,
        shares_delivered: progress.submitted_share_count,
        shares_confirmed: progress.confirmed_share_count,
    })
}

/// Keep SDK reconciliation available after close, but refuse any new network POST.
pub struct VotingWindowTransport<T> {
    pub inner: T,
    pub vote_end_time: u64,
}
impl<T: ChainTransport> ChainTransport for VotingWindowTransport<T> {
    fn chain_get<'a>(
        &'a self,
        request: ChainHttpRequest,
    ) -> zcash_voting::chain_submission::ChainTransportFuture<'a> {
        self.inner.chain_get(request)
    }
    fn chain_post_json<'a>(
        &'a self,
        request: ChainHttpRequest,
        body: Vec<u8>,
    ) -> zcash_voting::chain_submission::ChainTransportFuture<'a> {
        self.chain_post_json_with_dispatch(
            request,
            body,
            zcash_voting::chain_submission::ChainPostDispatch::default(),
        )
    }
    fn chain_post_json_with_dispatch<'a>(
        &'a self,
        request: ChainHttpRequest,
        body: Vec<u8>,
        dispatch: zcash_voting::chain_submission::ChainPostDispatch,
    ) -> zcash_voting::chain_submission::ChainTransportFuture<'a> {
        Box::pin(async move {
            if unix_seconds().map_err(ChainTransportError::definitely_unsent)? >= self.vote_end_time
            {
                return Err(ChainTransportError::definitely_unsent(
                    "voting has ended; saved transaction retained for reconciliation",
                ));
            }
            self.inner
                .chain_post_json_with_dispatch(request, body, dispatch)
                .await
        })
    }
}

pub fn round_binding(round: &RoundSnapshot, hotkey: &VotingHotkey) -> RoundBinding {
    RoundBinding {
        round_id: round.round_id.clone(),
        network: round.profile.network(),
        proposals: round
            .proposals
            .iter()
            .map(|p| ProposalRosterEntry {
                proposal_id: p.id,
                num_options: p.options.len() as u32,
            })
            .collect(),
        hotkey_secret: Some(Zeroizing::new(hotkey.stored_secret().to_vec())),
    }
}

pub async fn drive_round<T: ChainTransport>(
    executor: &RoundExecutor<T>,
    round: &RoundSnapshot,
    choices: &[VoteChoiceInput],
    hotkey: &VotingHotkey,
    tree_transport: Option<Arc<dyn vote_commitment_tree_client::transport::Transport>>,
    app: Option<&AppHandle>,
) -> Result<RoundRunReport, String> {
    validate_choices(round, choices)?;
    let now = unix_seconds()?;
    let active = round.profile.is_demo() || (round.is_active && now < round.vote_end_time);
    if active {
        executor
            .set_ballot_intents(
                &choices
                    .iter()
                    .map(|c| BallotIntent {
                        proposal_id: c.proposal_id,
                        decision: Decision::Choice(c.choice),
                    })
                    .collect::<Vec<_>>(),
            )
            .map_err(|e| e.to_string())?;
    } else {
        let plan = executor.plan().map_err(|e| e.to_string())?;
        if now < round.vote_end_time
            && plan
                .next_steps
                .iter()
                .any(|s| matches!(s, NextStep::CastVote { .. } | NextStep::Delegate { .. }))
        {
            return Err("this round is not active for casting".into());
        }
        // Expired rounds keep their durable intents. The executor checks the
        // refreshed deadline before each new cast while reconciling old work.
    }
    let control = ChainSubmissionControl::new(0);
    let host_control = control.clone();
    let host = RoundHostSourceBridge::new(|| {
        let now = unix_seconds().unwrap_or_else(|_| {
            host_control.cancel();
            0
        });
        RoundHostContext {
            configured_helper_urls: server_urls(round),
            now_seconds: if round.profile.is_demo() {
                round.vote_end_time - 1
            } else {
                now
            },
            ceremony_start_seconds: round.ceremony_start_time,
            vote_end_time_seconds: Some(round.vote_end_time),
            vote_tree_node_urls: server_urls(round),
            delegation: None,
            chain_policy: ChainAdvancePolicy::default(),
            max_proof_concurrency: zcash_voting::vote::DEFAULT_BATCH_PROOF_CONCURRENCY,
        }
    });
    let reporter = RoundDriveReporterBridge::new(|event| {
        if let Some(app) = app {
            report_progress(app, &round.round_id, event);
        }
    });
    // v5.0.0's fresh CastVote executor validates custodian-only TX1 fields
    // (rho_signed) absent from a canonical imported capability. Prepare through
    // the public custody-compatible proof API instead; all network effects and
    // recovery still belong to the SDK. One dispatch per pass lets us prepare
    // newly unlocked drafts after an older singleton finishes, without ever
    // sending an imported bundle down that custodian-only path.
    let mut aggregate: Option<RoundRunReport> = None;
    for _ in 0..512 {
        if active {
            prepare_custody_votes(executor, round, hotkey, tree_transport.clone(), app).await?;
        }
        let mut report = RoundDriver::new(executor)
            .with_policy(RoundDrivePolicy {
                max_dispatches: 1,
                progress_baseline: ProgressBaseline::SelectedChoices,
                ..Default::default()
            })
            .run(&host, &control, &reporter)
            .await;
        if let Some(mut previous) = aggregate.take() {
            previous.failures.append(&mut report.failures);
            report.failures = previous.failures;
            previous.chain_outcomes.append(&mut report.chain_outcomes);
            report.chain_outcomes = previous.chain_outcomes;
            previous
                .share_deliveries
                .append(&mut report.share_deliveries);
            report.share_deliveries = previous.share_deliveries;
            previous.delegations.append(&mut report.delegations);
            report.delegations = previous.delegations;
            previous.skipped_bundles.append(&mut report.skipped_bundles);
            report.skipped_bundles = previous.skipped_bundles;
        }
        if !matches!(
            report.quiescence,
            RoundQuiescence::PassBudgetExhausted { .. }
        ) || !report.failures.is_empty()
        {
            return Ok(report);
        }
        aggregate = Some(report);
    }
    aggregate.ok_or_else(|| "voting driver returned no report".into())
}

async fn prepare_custody_votes<T: ChainTransport>(
    executor: &RoundExecutor<T>,
    round: &RoundSnapshot,
    hotkey: &VotingHotkey,
    transport: Option<Arc<dyn vote_commitment_tree_client::transport::Transport>>,
    app: Option<&AppHandle>,
) -> Result<(), String> {
    let db = executor.database();
    let plan = executor.plan().map_err(|e| e.to_string())?;
    if plan
        .delegation_statuses
        .iter()
        .any(|s| s.phase != zcash_voting::phases::DelegationPhase::Confirmed)
    {
        return Ok(());
    }
    let mut bundles = BTreeMap::<u32, Vec<DraftVote>>::new();
    let now = unix_seconds()?;
    if !round.profile.is_demo() && now >= round.vote_end_time {
        return Ok(());
    }
    let single_share = round.profile.is_demo()
        || round.ceremony_start_time.is_some_and(|start| {
            zcash_voting::share_policy::is_last_moment(now, start, round.vote_end_time)
        });
    for step in plan.next_steps {
        if let NextStep::CastVote {
            bundle_index,
            proposal_id,
            choice,
        } = step
        {
            let proposal = round
                .proposals
                .iter()
                .find(|p| p.id == proposal_id)
                .ok_or("proposal is absent from authenticated roster")?;
            bundles.entry(bundle_index).or_default().push(DraftVote {
                proposal_id,
                choice,
                num_options: proposal.options.len() as u32,
                vc_tree_position: 0,
                single_share,
            });
        }
    }
    for (bundle_index, drafts) in bundles {
        let db = Arc::clone(&db);
        let round = round.clone();
        let transport = transport.clone();
        let secret = Zeroizing::new(hotkey.stored_secret().to_vec());
        let app = app.cloned();
        tauri::async_runtime::spawn_blocking(move || {
            let mut witness = None;
            let mut errors = Vec::new();
            for server in &round.vote_servers {
                let synced = match &transport {
                    Some(transport) => zcash_voting::precompute::sync_vote_tree_with(
                        &db,
                        &round.round_id,
                        &server.url,
                        Arc::clone(transport),
                    ),
                    None => {
                        zcash_voting::precompute::sync_vote_tree(&db, &round.round_id, &server.url)
                    }
                }
                .and_then(|height| {
                    zcash_voting::precompute::van_witness(
                        &db,
                        &round.round_id,
                        bundle_index,
                        height,
                    )
                });
                match synced {
                    Ok(value) => {
                        witness = Some(value);
                        break;
                    }
                    Err(error) => {
                        errors.push(error.to_string());
                        zcash_voting::precompute::reset_vote_tree(&db, &round.round_id)
                            .map_err(|e| e.to_string())?;
                    }
                }
            }
            let witness = witness.ok_or_else(|| {
                format!("vote-tree synchronization failed: {}", errors.join("; "))
            })?;
            if !round.profile.is_demo() && unix_seconds()? >= round.vote_end_time {
                return Err("voting ended before proof generation".into());
            }
            let hotkey = VotingHotkey::from_stored_secret(&secret, round.profile.network())
                .map_err(|e| e.to_string())?;
            let progress_round = round.round_id.clone();
            let progress_step = NextStep::CastVote {
                bundle_index,
                proposal_id: drafts[0].proposal_id,
                choice: drafts[0].choice,
            };
            let reporter = VoteCommitStageBridge::new(move |stage| {
                if let Some(app) = &app {
                    report_progress(
                        app,
                        &progress_round,
                        RoundDriveEvent::StepProgress {
                            step: progress_step.clone(),
                            progress: RoundStepProgress::VoteCommit(stage),
                        },
                    );
                }
            });
            let prepared = zcash_voting::vote::prepare_vote_work(
                &db,
                VoteSigner::hotkey(&hotkey),
                zcash_voting::vote::VoteWorkRequest {
                    round_id: &round.round_id,
                    bundle_index,
                    drafts: &drafts,
                    witness: &witness,
                    stages: &reporter,
                    max_proof_concurrency: zcash_voting::vote::DEFAULT_BATCH_PROOF_CONCURRENCY,
                },
            )
            .map_err(|e| format!("prepare custody vote failed: {e}"))?;
            zcash_voting::vote::persist_prepared_vote_work(&db, prepared)
                .map_err(|e| format!("persist custody vote failed: {e}"))?;
            Ok::<_, String>(())
        })
        .await
        .map_err(|e| format!("custody proof task failed: {e}"))??;
    }
    Ok(())
}

fn describe_run(report: &RoundRunReport) -> (String, String) {
    let (outcome, message) = match &report.quiescence {
        RoundQuiescence::NoWorkLeft => ("complete", "All votes and helper shares are confirmed."),
        RoundQuiescence::BackgroundShareWorkOnly { .. } => (
            "tracking",
            "Votes are confirmed. Helper shares are being tracked; keep this workspace open.",
        ),
        RoundQuiescence::Cancelled => (
            "pending",
            "Voting paused. Reopen this round to resume saved work.",
        ),
        RoundQuiescence::ChainRecoveryStalled { .. }
        | RoundQuiescence::PassBudgetExhausted { .. } => (
            "pending",
            "Submission is still unresolved. Retry to reconcile the saved transaction.",
        ),
        _ => (
            "blocked",
            "Voting needs attention. Saved progress has been retained.",
        ),
    };
    let details = report
        .failures
        .iter()
        .map(|f| f.failure.message.as_str())
        .collect::<Vec<_>>()
        .join("; ");
    (
        outcome.into(),
        if details.is_empty() {
            message.into()
        } else {
            format!("{message} {details}")
        },
    )
}

fn report_progress(app: &AppHandle, round_id: &str, event: RoundDriveEvent) {
    let (phase, bundle, proposal, fraction, message) = match event {
        RoundDriveEvent::PlanRefreshed { tally, .. } => (
            "progress",
            None,
            None,
            (tally.total_proposals > 0)
                .then(|| tally.completed_proposals as f64 / tally.total_proposals as f64),
            format!(
                "{} of {} selected proposals submitted",
                tally.completed_proposals, tally.total_proposals
            ),
        ),
        RoundDriveEvent::StepProgress {
            progress: RoundStepProgress::VoteCommit(stage),
            ..
        } => match stage {
            VoteCommitStage::ProofStarting {
                proposal_id,
                bundle_index,
            } => (
                "proving",
                Some(bundle_index),
                Some(proposal_id),
                Some(0.0),
                "Generating a zero-knowledge vote proof".into(),
            ),
            VoteCommitStage::ProofProgress {
                proposal_id,
                bundle_index,
                progress,
            } => (
                "proving",
                Some(bundle_index),
                Some(proposal_id),
                Some(progress),
                "Generating a zero-knowledge vote proof".into(),
            ),
            _ => return,
        },
        RoundDriveEvent::StepSelected { step } => {
            let Ok(view) = zcash_voting::wire::NextStepView::try_from(step) else {
                return;
            };
            (
                "working",
                Some(view.bundle_index),
                Some(view.proposal_id),
                None,
                "Advancing saved voting work".into(),
            )
        }
        RoundDriveEvent::StepFailed { message, .. } => ("pending", None, None, None, message),
        _ => return,
    };
    emit_progress(app, round_id, phase, bundle, proposal, fraction, message);
}

fn validate_choices(round: &RoundSnapshot, choices: &[VoteChoiceInput]) -> Result<(), String> {
    if choices.len() != round.proposals.len() {
        return Err("select exactly one option for every proposal".to_string());
    }
    let mut by_id = BTreeMap::new();
    for choice in choices {
        if by_id.insert(choice.proposal_id, choice.choice).is_some() {
            return Err(format!(
                "proposal {} appears more than once",
                choice.proposal_id
            ));
        }
    }
    for proposal in &round.proposals {
        let choice = by_id
            .get(&proposal.id)
            .ok_or_else(|| format!("proposal {} has no selected option", proposal.id))?;
        if *choice >= proposal.options.len() as u32 {
            return Err(format!(
                "proposal {} has an invalid selected option",
                proposal.id
            ));
        }
    }
    Ok(())
}

fn require_round_context(profile: Profile, round: &RoundSnapshot) -> Result<(), String> {
    if round.profile != profile
        || round.chain_id != profile.chain_id()
        || round.params.vote_round_id != round.round_id
    {
        return Err("round does not match the selected network profile".to_string());
    }
    if !round.authenticated {
        return Err("round is not authenticated".to_string());
    }
    Ok(())
}

pub fn validate_stored_round(stored: &StoredRound, fresh: &RoundSnapshot) -> Result<(), String> {
    let original = &stored.snapshot;
    if original.profile != fresh.profile
        || original.chain_id != fresh.chain_id
        || original.round_id != fresh.round_id
        || original.params != fresh.params
        || original.snapshot_height != fresh.snapshot_height
        || original.vote_end_time != fresh.vote_end_time
        || original.title != fresh.title
        || original.description != fresh.description
        || original.proposals != fresh.proposals
    {
        return Err(
            "authenticated round identity or proposal content changed after the customer target was created; voting is blocked"
                .to_string(),
        );
    }
    Ok(())
}

fn round_accepts_setup(profile: Profile, round: &RoundSnapshot, now: u64) -> bool {
    if profile.is_demo() {
        return true;
    }
    if round.vote_end_time <= now {
        return false;
    }
    if round.is_active {
        return true;
    }
    matches!(
        round.status.trim().to_ascii_lowercase().as_str(),
        "4" | "pending" | "session_status_pending"
    )
}

fn emit_progress(
    app: &AppHandle,
    round_id: &str,
    phase: &str,
    bundle_index: Option<u32>,
    proposal_id: Option<u32>,
    progress: Option<f64>,
    message: String,
) {
    let _ = app.emit(
        "vote-progress",
        VoteProgressEvent {
            round_id: round_id.to_string(),
            phase: phase.to_string(),
            bundle_index,
            proposal_id,
            progress,
            message,
        },
    );
}

fn unix_seconds() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| format!("system clock is before Unix epoch: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_round_allows_lifecycle_changes_but_rejects_proposal_changes() {
        let original = crate::demo::round_snapshot();
        let stored = StoredRound {
            snapshot: original.clone(),
            target_json: "public-target".to_string(),
            capability_digest: None,
        };
        let mut lifecycle_update = original.clone();
        lifecycle_update.status = "finalized".to_string();
        lifecycle_update.status_label = "Finalized".to_string();
        lifecycle_update.is_active = false;
        assert!(validate_stored_round(&stored, &lifecycle_update).is_ok());

        let mut changed_proposal = original;
        changed_proposal.proposals[0].options[0].label = "Changed choice".to_string();
        assert!(validate_stored_round(&stored, &changed_proposal).is_err());
    }

    #[test]
    fn new_target_setup_requires_a_future_voting_deadline() {
        let mut round = crate::demo::round_snapshot();
        round.vote_end_time = 100;

        assert!(round_accepts_setup(Profile::Testnet, &round, 99));
        assert!(!round_accepts_setup(Profile::Testnet, &round, 100));

        round.is_active = false;
        round.status = "pending".to_string();
        assert!(round_accepts_setup(Profile::Testnet, &round, 99));
        assert!(!round_accepts_setup(Profile::Testnet, &round, 100));

        round.status = "finalized".to_string();
        assert!(!round_accepts_setup(Profile::Testnet, &round, 99));
        assert!(round_accepts_setup(Profile::Demo, &round, 100));
    }

    #[test]
    fn manifest_failure_preserves_imported_capability_database_state() {
        let root = std::env::temp_dir().join(format!(
            "custody-voter-capability-rollback-{}",
            uuid::Uuid::new_v4()
        ));
        let paths = crate::storage::profile_paths(&root, Profile::Testnet).unwrap();
        let db = open_db(&paths, Profile::Testnet).unwrap();
        let wallet = db.wallet_id().to_string();
        db.conn()
            .execute(
                "INSERT INTO rounds (
                    round_id, wallet_id, network, snapshot_height, ea_pk, nc_root,
                    nullifier_imt_root, created_at
                 ) VALUES ('round', ?1, 'testnet', 1, X'00', X'00', X'00', 1)",
                [&wallet],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO bundles (
                    round_id, wallet_id, bundle_index, total_note_value, van_leaf_position
                 ) VALUES ('round', ?1, 0, 0, 7)",
                [&wallet],
            )
            .unwrap();
        std::fs::create_dir(&paths.manifest).unwrap();

        let error = persist_capability_manifest(
            &paths,
            &crate::model::ProfileManifest::empty(Profile::Testnet),
        )
        .unwrap_err();

        assert!(error.contains("replace file failed"));
        assert_eq!(db.get_bundle_count("round").unwrap(), 1);
        assert!(db.round("round").unwrap().is_some());
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
}
