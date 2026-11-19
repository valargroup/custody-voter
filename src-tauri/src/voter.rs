use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use rusqlite::named_params;
use tauri::{AppHandle, Emitter};
use zcash_voting::{
    delegation_capability::{
        import_delegation_capability, ImportDelegationCapabilityParams,
        MAX_DELEGATION_CAPABILITY_JSON_BYTES,
    },
    helper::url::canonicalize_helper_base_url,
    hotkey::generate_random_voting_hotkey,
    phases::DelegationPhase,
    round::VotingDb,
    session::{resume_plan, Decision, DelegationStatus, NextStep, RoundPlan},
    vote::VoteCommitStage,
    wire::VotingHotkeyTargetV1,
    AdvanceImportedDelegation, ChainAdvancePolicy, ChainSubmissionClient,
    ChainSubmissionClientConfig, ChainSubmissionControl, ChainSubmissionResult, ChainTransport,
    HelperClient, HelperHealth, ProgressBaseline, ProposalRosterEntry, RoundBinding,
    RoundDriveEvent, RoundDrivePolicy, RoundDriveReporterBridge, RoundDriver, RoundExecutor,
    RoundHostContext, RoundHostSourceBridge, RoundQuiescence, RoundRunReport, RoundStepFailureKind,
    RoundStepProgress, ShareDelegationRecord, VotingHotkey, VotingRoundParams,
};
use zeroize::Zeroizing;

use crate::{
    chain::VotingTransport,
    model::{
        CastVotesResult, ImportResult, Profile, RoundSnapshot, StoredRound, TargetResult,
        VoteChoiceInput, VoteProgressEvent, VoteTransactionResult,
    },
    storage::{
        load_hotkey, open_db, read_manifest, store_hotkey, vote_records, write_manifest,
        ProfilePaths, RECORDED_VOTE,
    },
};

/// Vote proofs one bundle's atomic batch may build at once.
const MAX_PROOF_CONCURRENCY: usize = 2;

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
    transport: Arc<VotingTransport>,
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
    let had_imported_capability = db
        .get_bundle_count(&round.round_id)
        .map_err(|error| format!("load existing custody payload state failed: {error}"))?
        > 0;
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
    persist_capability_manifest(
        paths,
        &manifest,
        &db,
        &round.round_id,
        had_imported_capability,
    )?;

    if profile.is_demo() {
        // The demo keeps confirmation as an explicit customer-visible step.
    } else {
        advance_imported_delegations(&db, transport, profile, &round).await?;
    }
    import_result(&db, &round, digest)
}

fn persist_capability_manifest(
    paths: &ProfilePaths,
    manifest: &crate::model::ProfileManifest,
    db: &VotingDb,
    round_id: &str,
    had_imported_capability: bool,
) -> Result<(), String> {
    if let Err(error) = write_manifest(paths, manifest) {
        if had_imported_capability {
            return Err(format!(
                "{error}; retry the same custody payload to finish local recovery"
            ));
        }
        // The checked deletion refuses a round whose delegation has reached the
        // chain, which every imported bundle has. Nothing is lost by discarding
        // it here: the import just ran, nothing was voted, and the custody
        // payload the customer still holds recreates the same state.
        return match db.delete_round_discarding_recovery(round_id) {
            Ok(()) => Err(error),
            Err(rollback_error) => Err(format!(
                "{error}; rolling back the imported custody payload also failed: {rollback_error}"
            )),
        };
    }
    Ok(())
}

pub async fn refresh_delegations(
    transport: Arc<VotingTransport>,
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
        crate::demo::confirm_delegations(&db, round).await?;
    } else {
        advance_imported_delegations(&db, transport, profile, round).await?;
    }
    import_result(&db, round, digest)
}

/// Polls every imported delegation that is not yet confirmed, once.
///
/// The SDK adopts the custody payload's transaction hash, reads the
/// transaction, and records the confirmed VAN position itself. A delegation
/// still pending stays pending; a terminal one is reported as an error.
pub async fn advance_imported_delegations<T: ChainTransport + Clone>(
    db: &Arc<VotingDb>,
    transport: T,
    profile: Profile,
    round: &RoundSnapshot,
) -> Result<(), String> {
    let plan = round_plan(db, round)?;
    ensure_no_terminal_delegation(&plan.delegation_statuses)?;
    let pending = plan
        .next_steps
        .iter()
        .filter_map(|step| match step {
            NextStep::AdvanceImportedDelegation { bundle_index } => Some(*bundle_index),
            _ => None,
        })
        .collect::<Vec<_>>();
    if pending.is_empty() {
        return Ok(());
    }
    // The SDK settles a lookup on the first server with a committed answer,
    // even one it then rejects for a missing confirmation event, so one such
    // server would fail every check. Asking each server on its own lets the
    // others answer.
    let config = chain_config(profile, round)?;
    let clients = config
        .endpoints
        .iter()
        .map(|endpoint| {
            let mut single = config.clone();
            single.endpoints = vec![endpoint.clone()];
            ChainSubmissionClient::with_transport(Arc::clone(db), transport.clone(), single)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|failure| format!("configure delegation polling failed: {}", failure.message()))?;
    let control = ChainSubmissionControl::new(0);
    let vote_round_id = round_id_bytes(&round.round_id)?;
    for bundle_index in pending {
        let request = AdvanceImportedDelegation {
            vote_round_id,
            bundle_index,
        };
        let mut answered = false;
        let mut last_failure = None;
        for client in &clients {
            match client.advance_imported_delegation(request, &control).await {
                Ok(ChainSubmissionResult::Confirmed(_)) => {
                    answered = true;
                    break;
                }
                Ok(ChainSubmissionResult::Pending(_)) => answered = true,
                Ok(
                    ChainSubmissionResult::Rejected(diagnostic)
                    | ChainSubmissionResult::SubmittedWithoutHash(diagnostic),
                ) => {
                    return Err(format!(
                        "custody delegation for bundle {bundle_index} cannot confirm: {}",
                        diagnostic.message()
                    ));
                }
                Ok(ChainSubmissionResult::Cancelled) => {
                    return Err("checking custody delegations was cancelled".to_string());
                }
                Err(failure) => last_failure = Some(failure),
            }
        }
        if let (false, Some(failure)) = (answered, last_failure) {
            return Err(format!(
                "check custody delegation for bundle {bundle_index} failed: {}",
                failure.message()
            ));
        }
    }
    Ok(())
}

fn ensure_no_terminal_delegation(statuses: &[DelegationStatus]) -> Result<(), String> {
    match statuses.iter().find(|status| status.terminal) {
        Some(status) => Err(format!(
            "custody delegation for bundle {} cannot confirm: {}",
            status.bundle_index,
            status
                .submission_diagnostic
                .as_ref()
                .map(|diagnostic| diagnostic.message())
                .unwrap_or("the vote chain did not accept it")
        )),
        None => Ok(()),
    }
}

fn import_result(
    db: &VotingDb,
    round: &RoundSnapshot,
    digest: String,
) -> Result<ImportResult, String> {
    let statuses = round_plan(db, round)?.delegation_statuses;
    let confirmed_bundle_count = statuses
        .iter()
        .filter(|status| status.phase == DelegationPhase::Confirmed)
        .count();
    Ok(ImportResult {
        digest,
        bundle_count: u32::try_from(statuses.len())
            .map_err(|_| "bundle count exceeds u32".to_string())?,
        confirmed_bundle_count: u32::try_from(confirmed_bundle_count)
            .map_err(|_| "confirmed bundle count exceeds u32".to_string())?,
        pending_transaction_hashes: statuses
            .into_iter()
            .filter(|status| status.phase != DelegationPhase::Confirmed)
            .filter_map(|status| status.tx_hash)
            .collect(),
    })
}

pub fn demo_capability(_paths: &ProfilePaths, round_id: &str) -> Result<String, String> {
    let round = crate::demo::round_snapshot();
    if round.round_id != round_id {
        return Err("unknown local demo round".to_string());
    }
    let hotkey = load_hotkey(Profile::Demo, round_id)?;
    crate::demo::capability_json(&hotkey)
}

/// Records the ballot and drives the round until every vote is confirmed and
/// its helper shares are handed off.
///
/// `on_share_delivery` runs each time the driver finishes delivering one
/// vote's shares, so background tracking can start while other bundles are
/// still being proved.
pub async fn cast_votes(
    app: AppHandle,
    transport: Arc<VotingTransport>,
    paths: ProfilePaths,
    profile: Profile,
    round: RoundSnapshot,
    choices: Vec<VoteChoiceInput>,
    on_share_delivery: &(dyn Fn() + Send + Sync),
) -> Result<CastVotesResult, String> {
    require_round_context(profile, &round)?;
    validate_choices(&round, &choices)?;

    let manifest = read_manifest(&paths, profile)?;
    let stored = manifest
        .rounds
        .get(&round.round_id)
        .ok_or_else(|| "generate a voting target before voting".to_string())?;
    validate_stored_round(stored, &round)?;
    if stored.capability_digest.is_none() {
        return Err("import the custody payload before voting".to_string());
    }
    let hotkey = load_hotkey(profile, &round.round_id)?;

    let db = Arc::new(open_db(&paths, profile)?);
    if profile.is_demo() {
        crate::demo::confirm_delegations(&db, &round).await?;
    } else {
        advance_imported_delegations(&db, Arc::clone(&transport), profile, &round).await?;
    }
    let statuses = round_plan(&db, &round)?.delegation_statuses;
    let pending = statuses
        .iter()
        .filter(|status| status.phase != DelegationPhase::Confirmed)
        .map(|status| status.bundle_index.to_string())
        .collect::<Vec<_>>();
    if statuses.is_empty() {
        return Err("import the custody payload before voting".to_string());
    }
    if !pending.is_empty() {
        return Err(format!(
            "custody delegation is still unconfirmed for bundle(s) {}; try Check confirmation again shortly",
            pending.join(", ")
        ));
    }
    let expected_votes = statuses.len() * choices.len();
    if !profile.is_demo() {
        let committed = committed_votes(&db, &round.round_id)?;
        ensure_round_accepts_votes(
            &round,
            unix_seconds()?,
            &committed,
            &choices,
            expected_votes,
        )?;
    }

    emit_progress(
        &app,
        &round.round_id,
        "starting",
        None,
        None,
        None,
        format!(
            "Preparing {expected_votes} proof-backed vote{}",
            if expected_votes == 1 { "" } else { "s" }
        ),
    );

    // The SDK casts only once every proposal has a terminal decision, and it
    // refuses a changed choice for a vote that already reached the chain.
    let intents = choices
        .iter()
        .map(|choice| {
            (
                choice.proposal_id,
                Decision::Choice(choice.choice),
                num_options(&round, choice.proposal_id),
            )
        })
        .collect::<Vec<_>>();
    db.set_ballot_intents(&round.round_id, profile.network(), &intents)
        .map_err(|error| format!("record ballot choices failed: {error}"))?;

    let transactions = if profile.is_demo() {
        crate::demo::cast_votes(&app, &db, &round, &hotkey).await?
    } else {
        let report = drive_round(
            &app,
            &db,
            transport,
            profile,
            &round,
            &hotkey,
            on_share_delivery,
        )
        .await?;
        ensure_round_completed(&report)?;
        vote_transactions(&paths, profile, &round)?
    };
    emit_progress(
        &app,
        &round.round_id,
        "complete",
        None,
        None,
        Some(1.0),
        if profile.is_demo() {
            "Local proof rehearsal completed. Nothing was broadcast.".to_string()
        } else {
            "All votes were confirmed and helper shares were submitted.".to_string()
        },
    );
    Ok(CastVotesResult {
        demo: profile.is_demo(),
        proof_count: u32::try_from(transactions.len())
            .map_err(|_| "vote count exceeds u32".to_string())?,
        transactions,
    })
}

/// Runs the SDK's round driver until the round needs nothing more from the
/// foreground.
///
/// The driver casts each bundle's choices, submits and confirms them on the
/// vote chain, plans and delivers the helper shares, and confirms the round's
/// immediate share. Share timing, helper placement, and retries all follow the
/// SDK's policy. The rest of the shares confirm in background tracking.
async fn drive_round(
    app: &AppHandle,
    db: &Arc<VotingDb>,
    transport: Arc<VotingTransport>,
    profile: Profile,
    round: &RoundSnapshot,
    hotkey: &VotingHotkey,
    on_share_delivery: &(dyn Fn() + Send + Sync),
) -> Result<RoundRunReport, String> {
    let executor = RoundExecutor::with_transport(
        Arc::clone(db),
        Arc::clone(&transport),
        chain_config(profile, round)?,
        HelperClient::new(transport.clone(), HelperHealth::default()),
    )
    .map_err(|failure| format!("configure vote submission failed: {}", failure.message()))?
    .with_binding(RoundBinding {
        round_id: round.round_id.clone(),
        network: profile.network(),
        proposals: round
            .proposals
            .iter()
            .map(|proposal| ProposalRosterEntry {
                proposal_id: proposal.id,
                num_options: proposal.options.len() as u32,
            })
            .collect(),
        hotkey_secret: Some(Zeroizing::new(hotkey.stored_secret().to_vec())),
    })
    .map_err(|error| format!("prepare the round for voting failed: {error}"))?
    .with_tree_transport(transport);

    let service_urls = service_urls(round);
    let ceremony_start_seconds = round.ceremony_start_time;
    let vote_end_time_seconds = vote_end_time(round);
    let host = RoundHostSourceBridge::new(move || RoundHostContext {
        configured_helper_urls: service_urls.clone(),
        // Read per dispatch: proofs can run long enough to cross the
        // last-moment window, which changes how shares are scheduled.
        now_seconds: unix_seconds().unwrap_or_default(),
        ceremony_start_seconds,
        vote_end_time_seconds,
        vote_tree_node_urls: service_urls.clone(),
        delegation: None,
        chain_policy: ChainAdvancePolicy::default(),
        max_proof_concurrency: MAX_PROOF_CONCURRENCY,
    });
    let progress_app = app.clone();
    let progress_round = round.round_id.clone();
    let reporter = RoundDriveReporterBridge::new(move |event| {
        // Only background tracking reconciles a share whose delivery outcome
        // is unknown, so it starts now rather than after the last bundle. A
        // slow bundle could otherwise hold it back past vote end.
        if matches!(
            &event,
            RoundDriveEvent::StepProgress {
                progress: RoundStepProgress::ShareOutcome(_),
                ..
            }
        ) {
            on_share_delivery();
        }
        report_drive_event(&progress_app, &progress_round, event);
    });
    // The SDK's default dispatch budget is a safety net sized for ordinary
    // ballots. A custody payload can hold thousands of bundles, and each needs
    // a cast, some chain re-polls, and helper delivery for every vote.
    let bundle_count = db
        .get_bundle_count(&round.round_id)
        .map_err(|error| format!("load delegated bundle count failed: {error}"))?;
    let defaults = RoundDrivePolicy::default();
    let policy = RoundDrivePolicy {
        progress_baseline: ProgressBaseline::SelectedChoices,
        max_dispatches: defaults
            .max_dispatches
            .max((bundle_count as usize).saturating_mul(round.proposals.len() + 8)),
        ..defaults
    };
    Ok(RoundDriver::new(&executor)
        .with_policy(policy)
        .run(&host, &ChainSubmissionControl::new(0), &reporter)
        .await)
}

/// Maps where a round run stopped to success or a customer-facing error.
fn ensure_round_completed(report: &RoundRunReport) -> Result<(), String> {
    match &report.quiescence {
        RoundQuiescence::NoWorkLeft | RoundQuiescence::BackgroundShareWorkOnly { .. } => Ok(()),
        RoundQuiescence::NeedsBallot {
            open_proposals,
            unrostered_intents,
        } => Err(format!(
            "the ballot is incomplete: open proposal(s) [{}], stored choices outside this round [{}]",
            join_ids(open_proposals),
            join_ids(unrostered_intents)
        )),
        RoundQuiescence::NeedsBundleSetup => {
            Err("the custody payload has no delegation bundles for this round".to_string())
        }
        RoundQuiescence::NeedsDelegationSignatures { bundles } => Err(format!(
            "custody delegation is not ready for bundle(s) {}",
            join_ids(bundles)
        )),
        RoundQuiescence::ChainTerminal { outcome, .. } => Err(match outcome {
            ChainSubmissionResult::Rejected(diagnostic) => {
                format!("the vote chain rejected a vote: {}", diagnostic.message())
            }
            ChainSubmissionResult::SubmittedWithoutHash(diagnostic) => format!(
                "a vote was submitted without a usable transaction hash and needs manual handling: {}",
                diagnostic.message()
            ),
            _ => "a vote submission ended without a confirmation".to_string(),
        }),
        RoundQuiescence::PersistedChainTerminal => Err(
            "a vote submission ended without a confirmation and needs manual handling"
                .to_string(),
        ),
        RoundQuiescence::ChainRecoveryStalled { .. } => Err(
            "the vote chain has not confirmed a submitted vote yet; retry shortly to resume safely"
                .to_string(),
        ),
        RoundQuiescence::Cancelled => {
            Err("vote submission was interrupted; retry to resume safely".to_string())
        }
        _ if !report.failures.is_empty()
            && report
                .failures
                .iter()
                .all(|record| record.failure.kind == RoundStepFailureKind::VoteEnded) =>
        {
            let bundles = report
                .failures
                .iter()
                .filter_map(|record| record.bundle_index)
                .collect::<Vec<_>>();
            Err(format!(
                "the voting window ended before bundle(s) {} could cast a vote",
                join_ids(&bundles)
            ))
        }
        _ => {
            let failures = report
                .failures
                .iter()
                .map(|record| match record.bundle_index {
                    Some(bundle_index) => {
                        format!("bundle {bundle_index}: {}", record.failure.message)
                    }
                    None => record.failure.message.clone(),
                })
                .collect::<Vec<_>>();
            Err(if failures.is_empty() {
                "vote submission stopped before every vote was confirmed; retry to resume safely"
                    .to_string()
            } else {
                format!(
                    "vote submission stopped before every vote was confirmed; retry to resume safely: {}",
                    failures.join("; ")
                )
            })
        }
    }
}

/// Translates driver events into the progress events the UI already shows.
fn report_drive_event(app: &AppHandle, round_id: &str, event: RoundDriveEvent) {
    let (phase, bundle_index, proposal_id, progress, message) = match event {
        RoundDriveEvent::StepSelected { step } => match step {
            NextStep::AdvanceImportedDelegation { bundle_index } => (
                "confirming_delegation",
                Some(bundle_index),
                None,
                None,
                "Checking the custody delegation on the vote chain",
            ),
            NextStep::CastVote {
                bundle_index,
                proposal_id,
                ..
            } => (
                "proving",
                Some(bundle_index),
                Some(proposal_id),
                None,
                "Preparing a proof-backed vote",
            ),
            NextStep::AdvanceVote {
                bundle_index,
                proposal_id,
            } => (
                "submitting_vote",
                Some(bundle_index),
                Some(proposal_id),
                None,
                "Submitting the signed vote to the vote chain",
            ),
            NextStep::AdvanceVoteBatch { bundle_index, .. } => (
                "submitting_vote",
                Some(bundle_index),
                None,
                None,
                "Submitting the bundle's signed votes to the vote chain",
            ),
            NextStep::SubmitShares {
                bundle_index,
                proposal_id,
                ..
            } => (
                "submitting_shares",
                Some(bundle_index),
                Some(proposal_id),
                None,
                "Submitting encrypted recovery shares",
            ),
            NextStep::ConfirmShare {
                bundle_index,
                proposal_id,
                ..
            } => (
                "confirming_shares",
                Some(bundle_index),
                Some(proposal_id),
                None,
                "Confirming helper share delivery",
            ),
            _ => return,
        },
        RoundDriveEvent::StepProgress { step, progress } => match progress {
            RoundStepProgress::TreeSynced { .. } => (
                "syncing_tree",
                Some(step_bundle(&step)),
                None,
                None,
                "Synchronized the authenticated vote tree",
            ),
            RoundStepProgress::VoteCommit(stage) => {
                return report_commit_stage(app, round_id, stage)
            }
            RoundStepProgress::ChainOutcome(ChainSubmissionResult::Pending(_)) => (
                "confirming_vote",
                Some(step_bundle(&step)),
                None,
                None,
                "Waiting for vote-chain confirmation",
            ),
            _ => return,
        },
        _ => return,
    };
    emit_progress(
        app,
        round_id,
        phase,
        bundle_index,
        proposal_id,
        progress,
        message.to_string(),
    );
}

/// Emits UI progress for one stage of building a vote proof.
pub fn report_commit_stage(app: &AppHandle, round_id: &str, stage: VoteCommitStage) {
    let (phase, bundle_index, proposal_id, progress, message) = match stage {
        VoteCommitStage::ProofStarting {
            proposal_id,
            bundle_index,
        } => (
            "proving",
            bundle_index,
            proposal_id,
            Some(0.0),
            "Generating a zero-knowledge vote proof",
        ),
        VoteCommitStage::ProofProgress {
            proposal_id,
            bundle_index,
            progress,
        } => (
            "proving",
            bundle_index,
            proposal_id,
            Some(progress),
            "Generating a zero-knowledge vote proof",
        ),
        VoteCommitStage::SharePayloadsBuilding {
            proposal_id,
            bundle_index,
        } => (
            "building_shares",
            bundle_index,
            proposal_id,
            Some(1.0),
            "Preparing encrypted recovery shares",
        ),
        VoteCommitStage::Signing {
            proposal_id,
            bundle_index,
        } => (
            "signing",
            bundle_index,
            proposal_id,
            None,
            "Signing the vote with the customer hotkey",
        ),
        _ => return,
    };
    emit_progress(
        app,
        round_id,
        phase,
        Some(bundle_index),
        Some(proposal_id),
        progress,
        message.to_string(),
    );
}

fn step_bundle(step: &NextStep) -> u32 {
    match step {
        NextStep::Delegate { bundle_index }
        | NextStep::AdvanceDelegation { bundle_index }
        | NextStep::AdvanceImportedDelegation { bundle_index }
        | NextStep::CastVote { bundle_index, .. }
        | NextStep::AdvanceVote { bundle_index, .. }
        | NextStep::AdvanceVoteBatch { bundle_index, .. }
        | NextStep::SubmitShares { bundle_index, .. }
        | NextStep::ConfirmShare { bundle_index, .. } => *bundle_index,
        _ => 0,
    }
}

/// Lists each confirmed vote with its transaction and helper share count.
fn vote_transactions(
    paths: &ProfilePaths,
    profile: Profile,
    round: &RoundSnapshot,
) -> Result<Vec<VoteTransactionResult>, String> {
    let db = open_db(paths, profile)?;
    let shares = zcash_voting::share::list(&db, &round.round_id)
        .map_err(|error| format!("load helper share delivery failed: {error}"))?;
    let helpers = canonical_helper_urls(&service_urls(round));
    let mut placed = BTreeMap::<(u32, u32), u32>::new();
    for share in shares
        .iter()
        .filter(|share| share_delivered(share, &helpers))
    {
        *placed
            .entry((share.bundle_index, share.proposal_id))
            .or_default() += 1;
    }
    Ok(vote_records(paths, profile, &round.round_id)?
        .into_iter()
        .filter_map(|vote| {
            Some(VoteTransactionResult {
                bundle_index: vote.bundle_index,
                proposal_id: vote.proposal_id,
                shares_submitted: placed
                    .get(&(vote.bundle_index, vote.proposal_id))
                    .copied()
                    .unwrap_or_default(),
                tx_hash: vote.tx_hash?,
                vc_tree_position: vote.vc_tree_position?,
            })
        })
        .collect())
}

/// Canonical forms of the configured helper URLs, as the SDK records them.
pub fn canonical_helper_urls(urls: &[String]) -> Vec<String> {
    urls.iter()
        .filter_map(|url| canonicalize_helper_base_url(url).ok())
        .collect()
}

/// Whether a configured helper definitely holds the share.
///
/// That is a helper's acknowledgement, or the helper quorum's confirmation,
/// which tracking can record without one when an acknowledgement was lost. An
/// outcome-unknown attempt alone does not count: tracking reconciles it, so
/// the share shows as pending until then. `configured` must hold canonical
/// URLs.
pub fn share_delivered(share: &ShareDelegationRecord, configured: &[String]) -> bool {
    share.confirmed
        || share
            .sent_to_urls
            .iter()
            .any(|url| configured.contains(url))
}

/// Limits a run to finishing committed votes once voting has closed.
///
/// Voting is closed past vote end, or while the chain reports the round
/// inactive. `committed` lists the proposal and choice of every vote the round
/// holds a commitment or confirmation for. Recording a changed choice discards
/// an unsent commitment, its whole batch included, and the SDK then casts
/// afresh, so a closed round refuses any changed choice: a signed vote that
/// could still be sent must not be lost to a cast that cannot happen. After
/// vote end the SDK refuses a fresh cast by itself, so committed votes finish
/// while a bundle that never cast fails alone. Before vote end it would cast,
/// so a round the chain reports inactive is refused while any vote is missing.
fn ensure_round_accepts_votes(
    round: &RoundSnapshot,
    now: u64,
    committed: &[(u32, u32)],
    choices: &[VoteChoiceInput],
    expected_votes: usize,
) -> Result<(), String> {
    let ended = round.vote_end_time <= now;
    if round.is_active && !ended {
        return Ok(());
    }
    let reused = committed
        .iter()
        .filter(|(proposal_id, choice)| {
            choices.iter().any(|requested| {
                requested.proposal_id == *proposal_id && requested.choice == *choice
            })
        })
        .count();
    if reused < committed.len() {
        return Err(
            "voting has closed, so a vote that is already signed can't be changed; keep its original choice to finish it"
                .to_string(),
        );
    }
    if reused >= expected_votes || (ended && reused > 0) {
        return Ok(());
    }
    Err(if round.is_active {
        "the voting window ended before this vote was broadcast".to_string()
    } else {
        format!(
            "round is {}, so a new vote cannot be created or broadcast",
            round.status_label
        )
    })
}

/// The proposal and choice of every vote the round records (see
/// [`RECORDED_VOTE`]).
fn committed_votes(db: &VotingDb, round_id: &str) -> Result<Vec<(u32, u32)>, String> {
    let wallet = db.wallet_id();
    let conn = db.conn();
    let mut statement = conn
        .prepare(&format!(
            "SELECT proposal_id, choice FROM votes
             WHERE round_id = :round_id AND wallet_id = :wallet_id AND {RECORDED_VOTE}"
        ))
        .map_err(|error| format!("prepare committed vote query failed: {error}"))?;
    let rows = statement
        .query_map(
            named_params! { ":round_id": round_id, ":wallet_id": wallet },
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| format!("query committed votes failed: {error}"))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("decode committed vote failed: {error}"))
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

fn num_options(round: &RoundSnapshot, proposal_id: u32) -> u32 {
    round
        .proposals
        .iter()
        .find(|proposal| proposal.id == proposal_id)
        .map_or(0, |proposal| proposal.options.len() as u32)
}

/// Reads the SDK's resume plan for the round's authenticated proposal roster.
pub fn round_plan(db: &VotingDb, round: &RoundSnapshot) -> Result<RoundPlan, String> {
    let proposal_ids = round
        .proposals
        .iter()
        .map(|proposal| proposal.id)
        .collect::<Vec<_>>();
    resume_plan(db, &round.round_id, &proposal_ids)
        .map_err(|error| format!("load voting progress failed: {error}"))
}

/// Vote-chain endpoints from the authenticated config. Each one serves the
/// chain API, the vote tree, and helper share intake.
pub fn service_urls(round: &RoundSnapshot) -> Vec<String> {
    round
        .vote_servers
        .iter()
        .map(|server| server.url.clone())
        .collect()
}

/// The round's authenticated vote end, or `None` if the chain reported none.
pub fn vote_end_time(round: &RoundSnapshot) -> Option<u64> {
    (round.vote_end_time > 0).then_some(round.vote_end_time)
}

fn chain_config(
    profile: Profile,
    round: &RoundSnapshot,
) -> Result<ChainSubmissionClientConfig, String> {
    // The demo round lists no vote servers. It talks to the in-process chain.
    let endpoints = if profile.is_demo() && round.vote_servers.is_empty() {
        vec![crate::demo::CHAIN_URL.to_string()]
    } else {
        service_urls(round)
    };
    if endpoints.is_empty() {
        return Err("authenticated config has no vote servers".to_string());
    }
    Ok(
        ChainSubmissionClientConfig::for_network(profile.network(), endpoints)
            .with_vote_chain_id(round.chain_id.clone()),
    )
}

fn round_id_bytes(round_id: &str) -> Result<[u8; 32], String> {
    hex::decode(round_id)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| "round id must be 32 bytes of hex".to_string())
}

fn join_ids(ids: &[u32]) -> String {
    ids.iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ")
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

pub fn emit_progress(
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

pub fn unix_seconds() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| format!("system clock is before Unix epoch: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::ServiceEndpointView;

    /// A vote chain whose first server answers a delegation lookup as
    /// committed but without its confirmation event.
    struct EventlessFirstServer {
        eventless: String,
        round_id: String,
        hashes: BTreeMap<String, u32>,
    }

    impl ChainTransport for EventlessFirstServer {
        fn chain_get<'a>(
            &'a self,
            request: zcash_voting::ChainHttpRequest,
        ) -> zcash_voting::ChainTransportFuture<'a> {
            let url = request.url();
            let events = match url
                .rsplit('/')
                .next()
                .and_then(|hash| self.hashes.get(hash))
            {
                Some(_) if url.starts_with(&self.eventless) => serde_json::json!([]),
                Some(bundle_index) => serde_json::json!([{
                    "type": "delegate_vote",
                    "attributes": [
                        { "key": "vote_round_id", "value": self.round_id },
                        { "key": "leaf_index", "value": bundle_index.to_string() },
                    ],
                }]),
                None => serde_json::Value::Null,
            };
            let response = if events.is_null() {
                zcash_voting::ChainHttpResponse::json(404, br#"{"error":"tx not found"}"#.to_vec())
            } else {
                let body =
                    serde_json::json!({"height": "1", "code": 0, "log": "", "events": events});
                zcash_voting::ChainHttpResponse::json(200, body.to_string().into_bytes())
            };
            Box::pin(async move { Ok(response) })
        }

        fn chain_post_json<'a>(
            &'a self,
            _request: zcash_voting::ChainHttpRequest,
            _json: Vec<u8>,
        ) -> zcash_voting::ChainTransportFuture<'a> {
            Box::pin(async {
                Err(zcash_voting::ChainTransportError::definitely_unsent(
                    "delegation polling never posts",
                ))
            })
        }
    }

    #[tokio::test]
    async fn delegation_lookup_fails_over_past_a_server_without_events() {
        let mut round = crate::demo::round_snapshot();
        round.vote_servers = ["http://eventless.invalid", "http://healthy.invalid"]
            .map(|url| ServiceEndpointView {
                url: url.to_string(),
                label: url.to_string(),
            })
            .to_vec();
        let hotkey = generate_random_voting_hotkey(Profile::Demo.network()).unwrap();
        let capability = crate::demo::capability_json(&hotkey).unwrap();
        let db = Arc::new(VotingDb::open_in_memory().unwrap());
        db.set_wallet_id("failover-test");
        import_delegation_capability(
            &db,
            capability.as_bytes(),
            ImportDelegationCapabilityParams {
                voting_hotkey: &hotkey,
                expected_chain_id: &round.chain_id,
                expected_network: Profile::Demo.network(),
                expected_round_params: &round.params,
                session_json: None,
            },
        )
        .unwrap();
        let hashes = zcash_voting::DelegationCapabilityV1::from_json(capability.as_bytes())
            .unwrap()
            .bundles
            .iter()
            .map(|bundle| (bundle.delegation_tx_hash.clone(), bundle.bundle_index))
            .collect();
        let chain = Arc::new(EventlessFirstServer {
            eventless: "http://eventless.invalid".to_string(),
            round_id: round.round_id.clone(),
            hashes,
        });

        advance_imported_delegations(&db, chain, Profile::Demo, &round)
            .await
            .unwrap();

        assert!(round_plan(&db, &round)
            .unwrap()
            .delegation_statuses
            .iter()
            .all(|status| status.phase == DelegationPhase::Confirmed));
    }

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
    fn closed_round_finishes_committed_votes_without_casting_new_ones() {
        let mut round = crate::demo::round_snapshot();
        round.vote_end_time = 100;
        let choices = [
            VoteChoiceInput {
                proposal_id: 1,
                choice: 0,
            },
            VoteChoiceInput {
                proposal_id: 2,
                choice: 1,
            },
        ];
        let changed = [
            VoteChoiceInput {
                proposal_id: 1,
                choice: 2,
            },
            choices[1].clone(),
        ];
        let committed = [(1, 0), (2, 1)];
        let accepts = |round: &RoundSnapshot, now, committed: &[(u32, u32)], choices| {
            ensure_round_accepts_votes(round, now, committed, choices, 2)
        };

        // An open round takes new and changed votes.
        assert!(accepts(&round, 99, &[], &choices).is_ok());
        assert!(accepts(&round, 99, &committed, &changed).is_ok());

        // Past vote end the SDK refuses fresh casts itself, so committed
        // votes finish, but a changed choice would discard a signed vote.
        assert!(accepts(&round, 101, &committed[..1], &choices).is_ok());
        assert!(accepts(&round, 101, &[], &choices)
            .unwrap_err()
            .contains("voting window ended"));
        assert!(accepts(&round, 101, &committed, &changed)
            .unwrap_err()
            .contains("can't be changed"));

        // Before vote end the SDK would cast, so an inactive round refuses any
        // missing vote.
        round.is_active = false;
        round.status_label = "Finalized".to_string();
        assert!(accepts(&round, 99, &committed, &choices).is_ok());
        assert!(accepts(&round, 99, &committed[..1], &choices)
            .unwrap_err()
            .contains("Finalized"));
        assert!(accepts(&round, 99, &committed, &changed)
            .unwrap_err()
            .contains("can't be changed"));
    }

    #[test]
    fn manifest_failure_rolls_back_new_capability_database_state() {
        let root = std::env::temp_dir().join(format!(
            "custody-voter-capability-rollback-{}",
            uuid::Uuid::new_v4()
        ));
        let paths = crate::storage::profile_paths(&root, Profile::Demo).unwrap();
        let round = crate::demo::round_snapshot();
        let hotkey = generate_random_voting_hotkey(Profile::Demo.network()).unwrap();
        let capability = crate::demo::capability_json(&hotkey).unwrap();
        let db = open_db(&paths, Profile::Demo).unwrap();
        import_delegation_capability(
            &db,
            capability.as_bytes(),
            ImportDelegationCapabilityParams {
                voting_hotkey: &hotkey,
                expected_chain_id: &round.chain_id,
                expected_network: Profile::Demo.network(),
                expected_round_params: &round.params,
                session_json: None,
            },
        )
        .unwrap();
        assert_eq!(db.get_bundle_count(&round.round_id).unwrap(), 2);
        std::fs::create_dir(&paths.manifest).unwrap();

        let error = persist_capability_manifest(
            &paths,
            &crate::model::ProfileManifest::empty(Profile::Demo),
            &db,
            &round.round_id,
            false,
        )
        .unwrap_err();

        assert!(error.contains("replace file failed"));
        assert_eq!(db.get_bundle_count(&round.round_id).unwrap(), 0);
        assert!(db.round(&round.round_id).unwrap().is_none());
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn helper_progress_counts_only_definite_placement() {
        let configured = canonical_helper_urls(&[
            "https://helper-a".to_string(),
            "https://helper-b/".to_string(),
        ]);
        let mut share = ShareDelegationRecord {
            round_id: "round".to_string(),
            bundle_index: 0,
            proposal_id: 1,
            share_index: 0,
            sent_to_urls: Vec::new(),
            ambiguous_urls: Vec::new(),
            attempting_urls: Vec::new(),
            target_count: 2,
            nullifier: vec![0; 32],
            confirmed: false,
            submit_at: 0,
            created_at: 0,
        };
        share.ambiguous_urls = vec!["https://helper-a".to_string()];
        share.attempting_urls = vec!["https://helper-b".to_string()];
        assert!(!share_delivered(&share, &configured));

        share.sent_to_urls = vec!["https://removed-helper".to_string()];
        assert!(!share_delivered(&share, &configured));

        share.sent_to_urls.push("https://helper-b".to_string());
        assert!(share_delivered(&share, &configured));

        // A lost acknowledgement that the quorum later confirmed still counts.
        share.sent_to_urls.clear();
        share.confirmed = true;
        assert!(share_delivered(&share, &configured));
    }
}
