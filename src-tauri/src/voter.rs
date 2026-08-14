use std::{
    collections::{BTreeMap, HashSet},
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use rand::{rngs::OsRng, RngCore};
use reqwest::Client;
use rusqlite::named_params;
use tauri::{AppHandle, Emitter};
use zcash_voting::{
    confirmation::{confirm_delegation_submission, confirm_vote_submission},
    delegation_capability::{
        import_delegation_capability, ImportDelegationCapabilityParams,
        MAX_DELEGATION_CAPABILITY_JSON_BYTES,
    },
    hotkey::generate_random_voting_hotkey,
    precompute::{reset_vote_tree, sync_vote_tree, van_witness},
    round::VotingDb,
    share_policy::{
        is_last_moment, last_moment_buffer_seconds, plan_share_submissions,
        share_submission_random_bytes_required, share_submission_target_count,
    },
    vote::{
        commit_batch, recover_signed_commitments, CommittedVote, DraftVote, SignedVoteCommitment,
        VanWitness, VoteCommitStage, VoteSigner,
    },
    wire::{VoteCommitmentWire, VoteShareWire, VotingHotkeyTargetV1},
    VoteCommitStageBridge, VotingHotkey, VotingRoundParams,
};
use zeroize::Zeroizing;

use crate::{
    chain,
    model::{
        CastVotesResult, ImportResult, Profile, RoundSnapshot, StoredRound, TargetResult,
        VoteChoiceInput, VoteProgressEvent, VoteTransactionResult,
    },
    storage::{load_hotkey, open_db, read_manifest, store_hotkey, write_manifest, ProfilePaths},
};

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
    let db = open_db(paths, profile)?;
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
        refresh_delegations_once(client, &db, &round).await?;
    }
    import_result(&db, &round.round_id, digest)
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
        return match db.delete_round(round_id) {
            Ok(()) => Err(error),
            Err(rollback_error) => Err(format!(
                "{error}; rolling back the imported custody payload also failed: {rollback_error}"
            )),
        };
    }
    Ok(())
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
    let db = open_db(paths, profile)?;
    if profile.is_demo() {
        crate::demo::confirm_delegations(&db, &round.round_id)?;
    } else {
        refresh_delegations_once(client, &db, round).await?;
    }
    import_result(&db, &round.round_id, digest)
}

async fn refresh_delegations_once(
    client: &Client,
    db: &VotingDb,
    round: &RoundSnapshot,
) -> Result<(), String> {
    let rows = delegation_rows(db, &round.round_id)?;
    for (bundle_index, tx_hash, position) in rows {
        if position.is_some() {
            continue;
        }
        let Some(confirmation) = chain::get_validated_tx_confirmation(
            client,
            &round.vote_servers,
            &tx_hash,
            |confirmation| {
                confirm_delegation_submission(
                    db,
                    &round.round_id,
                    bundle_index,
                    &tx_hash,
                    &confirmation.events,
                )
                .map(|_| ())
                .map_err(|error| format!("confirm custody delegation failed: {error}"))
            },
        )
        .await?
        else {
            continue;
        };
        if confirmation.code != 0 {
            return Err(if confirmation.log.trim().is_empty() {
                format!(
                    "delegation transaction {tx_hash} failed with code {}",
                    confirmation.code
                )
            } else {
                confirmation.log
            });
        }
    }
    Ok(())
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
    client: Client,
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
        .ok_or_else(|| "generate a voting target before voting".to_string())?;
    validate_stored_round(stored, &round)?;
    if stored.capability_digest.is_none() {
        return Err("import the custody payload before voting".to_string());
    }

    let db = open_db(&paths, profile)?;
    if profile.is_demo() {
        crate::demo::confirm_delegations(&db, &round.round_id)?;
    } else {
        refresh_delegations_once(&client, &db, &round).await?;
    }
    let pending = delegation_rows(&db, &round.round_id)?
        .into_iter()
        .filter_map(|(index, _, position)| position.is_none().then_some(index))
        .collect::<Vec<_>>();
    if !pending.is_empty() {
        return Err(format!(
            "custody delegation is still unconfirmed for bundle(s) {}; try Check confirmation again shortly",
            pending
                .iter()
                .map(u32::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let bundle_count = db
        .get_bundle_count(&round.round_id)
        .map_err(|error| format!("load delegated bundle count failed: {error}"))?;
    drop(db);

    emit_progress(
        &app,
        &round.round_id,
        "starting",
        None,
        None,
        None,
        format!(
            "Preparing {} proof-backed vote{}",
            bundle_count * choices.len() as u32,
            if bundle_count * choices.len() as u32 == 1 {
                ""
            } else {
                "s"
            }
        ),
    );

    let mut transactions = Vec::new();
    let mut proof_count = 0u32;
    for bundle_index in 0..bundle_count {
        for choice in &choices {
            let proposal = round
                .proposals
                .iter()
                .find(|proposal| proposal.id == choice.proposal_id)
                .expect("choices were validated against proposals");
            let existing = existing_vote(
                &paths,
                profile,
                &round.round_id,
                bundle_index,
                choice.proposal_id,
            )?;
            if let Some(existing) = &existing {
                if existing.choice != choice.choice {
                    return Err(format!(
                        "bundle {bundle_index}, proposal {} was already committed with a different choice",
                        choice.proposal_id
                    ));
                }
            }
            if !profile.is_demo() {
                ensure_vote_can_continue(profile, &round, existing.as_ref(), unix_seconds()?)?;
            }

            let single_share = if profile.is_demo() {
                true
            } else if let Some(start) = round.ceremony_start_time {
                is_last_moment(unix_seconds()?, start, round.vote_end_time)
            } else {
                false
            };
            let draft = DraftVote {
                proposal_id: choice.proposal_id,
                choice: choice.choice,
                num_options: proposal.options.len() as u32,
                vc_tree_position: 0,
                single_share,
            };
            let commitment = if existing.is_some() {
                emit_progress(
                    &app,
                    &round.round_id,
                    "recovering",
                    Some(bundle_index),
                    Some(choice.proposal_id),
                    None,
                    "Recovering the persisted signed vote".to_string(),
                );
                recover_one(
                    &paths,
                    profile,
                    &round.round_id,
                    bundle_index,
                    choice.proposal_id,
                )
                .await?
            } else {
                let witness = if profile.is_demo() {
                    let paths = paths.clone();
                    let round_id = round.round_id.clone();
                    tauri::async_runtime::spawn_blocking(move || {
                        let db = open_db(&paths, profile)?;
                        crate::demo::witness(&db, &round_id, bundle_index)
                    })
                    .await
                    .map_err(|error| format!("demo witness task failed: {error}"))??
                } else {
                    sync_and_witness(
                        app.clone(),
                        paths.clone(),
                        profile,
                        round.round_id.clone(),
                        bundle_index,
                        round.vote_servers.clone(),
                    )
                    .await?
                };
                build_commitment(
                    app.clone(),
                    paths.clone(),
                    profile,
                    round.round_id.clone(),
                    bundle_index,
                    draft,
                    witness,
                )
                .await?
            };
            proof_count = proof_count.saturating_add(1);

            let existing = existing_vote(
                &paths,
                profile,
                &round.round_id,
                bundle_index,
                choice.proposal_id,
            )?;
            let (tx_hash, vc_tree_position, shares_submitted) = if profile.is_demo() {
                finish_demo_vote(&paths, &round, bundle_index, &commitment, existing)?
            } else {
                finish_live_vote(
                    &app,
                    &client,
                    &paths,
                    profile,
                    &round,
                    bundle_index,
                    &commitment,
                    existing,
                    single_share,
                )
                .await?
            };
            transactions.push(VoteTransactionResult {
                bundle_index,
                proposal_id: choice.proposal_id,
                tx_hash,
                vc_tree_position,
                shares_submitted,
            });
        }
    }
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
        proof_count,
        transactions,
    })
}

#[derive(Clone, Debug)]
struct ExistingVote {
    choice: u32,
    tx_hash: Option<String>,
    vc_tree_position: Option<u64>,
}

fn existing_vote(
    paths: &ProfilePaths,
    profile: Profile,
    round_id: &str,
    bundle_index: u32,
    proposal_id: u32,
) -> Result<Option<ExistingVote>, String> {
    let db = open_db(paths, profile)?;
    let wallet = db.wallet_id();
    let conn = db.conn();
    conn.query_row(
        "SELECT choice, tx_hash, vc_tree_position
         FROM votes
         WHERE round_id = :round_id AND wallet_id = :wallet_id
           AND bundle_index = :bundle_index AND proposal_id = :proposal_id",
        named_params! {
            ":round_id": round_id,
            ":wallet_id": wallet,
            ":bundle_index": bundle_index,
            ":proposal_id": proposal_id,
        },
        |row| {
            Ok(ExistingVote {
                choice: row.get(0)?,
                tx_hash: row.get(1)?,
                vc_tree_position: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(|error| format!("load existing vote failed: {error}"))
}

use rusqlite::OptionalExtension;

async fn sync_and_witness(
    app: AppHandle,
    paths: ProfilePaths,
    profile: Profile,
    round_id: String,
    bundle_index: u32,
    servers: Vec<crate::model::ServiceEndpointView>,
) -> Result<VanWitness, String> {
    emit_progress(
        &app,
        &round_id,
        "syncing_tree",
        Some(bundle_index),
        None,
        None,
        "Synchronizing the authenticated vote tree".to_string(),
    );
    tauri::async_runtime::spawn_blocking(move || {
        let db = open_db(&paths, profile)?;
        let mut errors = Vec::new();
        for server in servers {
            match sync_vote_tree(&db, &round_id, &server.url) {
                Ok(height) => {
                    return van_witness(&db, &round_id, bundle_index, height)
                        .map_err(|error| format!("generate VAN witness failed: {error}"));
                }
                Err(error) => {
                    errors.push(format!("{}: {error}", server.label));
                    let _ = reset_vote_tree(&db, &round_id);
                }
            }
        }
        Err(format!(
            "all vote-tree servers failed: {}",
            errors.join("; ")
        ))
    })
    .await
    .map_err(|error| format!("vote-tree synchronization task failed: {error}"))?
}

async fn build_commitment(
    app: AppHandle,
    paths: ProfilePaths,
    profile: Profile,
    round_id: String,
    bundle_index: u32,
    draft: DraftVote,
    witness: VanWitness,
) -> Result<SignedVoteCommitment, String> {
    let hotkey = load_hotkey(profile, &round_id)?;
    let secret = Zeroizing::new(hotkey.stored_secret().to_vec());
    tauri::async_runtime::spawn_blocking(move || {
        let progress_app = app.clone();
        let progress_round = round_id.clone();
        let reporter = VoteCommitStageBridge::new(move |stage| {
            let (phase, proposal_id, bundle, progress, message) = match stage {
                VoteCommitStage::ProofStarting {
                    proposal_id,
                    bundle_index,
                } => (
                    "proving",
                    Some(proposal_id),
                    Some(bundle_index),
                    Some(0.0),
                    "Generating a zero-knowledge vote proof".to_string(),
                ),
                VoteCommitStage::ProofProgress {
                    proposal_id,
                    bundle_index,
                    progress,
                } => (
                    "proving",
                    Some(proposal_id),
                    Some(bundle_index),
                    Some(progress),
                    "Generating a zero-knowledge vote proof".to_string(),
                ),
                VoteCommitStage::SharePayloadsBuilding {
                    proposal_id,
                    bundle_index,
                } => (
                    "building_shares",
                    Some(proposal_id),
                    Some(bundle_index),
                    Some(1.0),
                    "Preparing encrypted recovery shares".to_string(),
                ),
                VoteCommitStage::Signing {
                    proposal_id,
                    bundle_index,
                } => (
                    "signing",
                    Some(proposal_id),
                    Some(bundle_index),
                    None,
                    "Signing the vote with the customer hotkey".to_string(),
                ),
                _ => ("proving", None, None, None, "Preparing vote".to_string()),
            };
            emit_progress(
                &progress_app,
                &progress_round,
                phase,
                bundle,
                proposal_id,
                progress,
                message,
            );
        });
        let db = open_db(&paths, profile)?;
        let hotkey = VotingHotkey::from_stored_secret(&secret, profile.network())
            .map_err(|error| format!("reconstruct voting hotkey failed: {error}"))?;
        let signed = commit_batch(
            &db,
            &round_id,
            bundle_index,
            &[draft],
            &witness,
            VoteSigner::hotkey(&hotkey),
            &reporter,
        )
        .map_err(|error| format!("build signed vote commitment failed: {error}"))?;
        signed
            .commitments
            .into_iter()
            .next()
            .ok_or_else(|| "vote proof builder returned no commitment".to_string())
    })
    .await
    .map_err(|error| format!("vote proof task failed: {error}"))?
}

async fn recover_one(
    paths: &ProfilePaths,
    profile: Profile,
    round_id: &str,
    bundle_index: u32,
    proposal_id: u32,
) -> Result<SignedVoteCommitment, String> {
    let paths = paths.clone();
    let round_id = round_id.to_string();
    tauri::async_runtime::spawn_blocking(move || {
        let db = open_db(&paths, profile)?;
        let recovered = recover_signed_commitments(&db, &round_id, bundle_index, proposal_id)
            .map_err(|error| format!("recover signed vote failed: {error}"))?;
        recovered
            .commitments
            .into_iter()
            .next()
            .ok_or_else(|| "recovered vote contains no commitment".to_string())
    })
    .await
    .map_err(|error| format!("vote recovery task failed: {error}"))?
}

fn finish_demo_vote(
    paths: &ProfilePaths,
    round: &RoundSnapshot,
    bundle_index: u32,
    commitment: &SignedVoteCommitment,
    existing: Option<ExistingVote>,
) -> Result<(String, u64, u32), String> {
    if let Some(existing) = existing {
        if let (Some(tx_hash), Some(vc_position)) = (existing.tx_hash, existing.vc_tree_position) {
            return Ok((tx_hash, vc_position, commitment.share_payloads.len() as u32));
        }
    }
    let db = open_db(paths, Profile::Demo)?;
    let (tx_hash, vc_position) =
        crate::demo::confirm_vote(&db, &round.round_id, bundle_index, commitment)?;
    Ok((tx_hash, vc_position, commitment.share_payloads.len() as u32))
}

#[allow(clippy::too_many_arguments)]
async fn finish_live_vote(
    app: &AppHandle,
    client: &Client,
    paths: &ProfilePaths,
    profile: Profile,
    round: &RoundSnapshot,
    bundle_index: u32,
    commitment: &SignedVoteCommitment,
    existing: Option<ExistingVote>,
    single_share: bool,
) -> Result<(String, u64, u32), String> {
    let mut tx_hash = existing.as_ref().and_then(|vote| vote.tx_hash.clone());
    let mut vc_position = existing.as_ref().and_then(|vote| vote.vc_tree_position);
    if tx_hash.is_none() {
        ensure_vote_can_continue(profile, round, None, unix_seconds()?)?;
        emit_progress(
            app,
            &round.round_id,
            "submitting_vote",
            Some(bundle_index),
            Some(commitment.proposal_id),
            None,
            "Submitting the signed vote to the vote chain".to_string(),
        );
        let wire = VoteCommitmentWire::try_from(commitment)
            .map_err(|error| format!("build vote-chain payload failed: {error}"))?;
        let body = wire
            .to_json()
            .map_err(|error| format!("serialize vote-chain payload failed: {error}"))?;
        let submitted = chain::submit_vote(client, &round.vote_servers, &body).await?;
        let db = open_db(paths, profile)?;
        let recovered =
            CommittedVote::recover(&db, &round.round_id, bundle_index, commitment.proposal_id)
                .map_err(|error| {
                    format!("recover vote for submission recording failed: {error}")
                })?;
        recovered
            .record_submission(&db, &submitted.tx_hash)
            .map_err(|error| format!("record vote submission failed: {error}"))?;
        tx_hash = Some(submitted.tx_hash);
    }
    let tx_hash = tx_hash.expect("vote hash was set above");
    if vc_position.is_none() {
        emit_progress(
            app,
            &round.round_id,
            "confirming_vote",
            Some(bundle_index),
            Some(commitment.proposal_id),
            None,
            "Waiting for vote-chain confirmation".to_string(),
        );
        let db = open_db(paths, profile)?;
        let confirmation = chain::poll_validated_tx_confirmation(
            client,
            &round.vote_servers,
            &tx_hash,
            |confirmation| {
                let parsed = confirm_vote_submission(
                    &db,
                    &round.round_id,
                    bundle_index,
                    commitment.proposal_id,
                    &tx_hash,
                    &confirmation.events,
                )
                .map_err(|error| format!("record vote confirmation failed: {error}"))?;
                vc_position = Some(parsed.vc_tree_position);
                Ok(())
            },
        )
        .await?;
        if confirmation.code != 0 {
            return Err(if confirmation.log.trim().is_empty() {
                format!("vote transaction failed with code {}", confirmation.code)
            } else {
                confirmation.log
            });
        }
    }
    let vc_position = vc_position.expect("vote confirmation set a VC position");
    emit_progress(
        app,
        &round.round_id,
        "submitting_shares",
        Some(bundle_index),
        Some(commitment.proposal_id),
        None,
        "Submitting encrypted recovery shares".to_string(),
    );
    let submitted_shares = submit_missing_shares(
        client,
        paths,
        profile,
        round,
        bundle_index,
        commitment,
        vc_position,
        single_share,
    )
    .await?;
    Ok((tx_hash, vc_position, submitted_shares))
}

fn ensure_vote_can_continue(
    profile: Profile,
    round: &RoundSnapshot,
    existing: Option<&ExistingVote>,
    now: u64,
) -> Result<(), String> {
    if profile.is_demo() || existing.and_then(|vote| vote.tx_hash.as_ref()).is_some() {
        return Ok(());
    }
    if !round.is_active {
        return Err(format!(
            "round is {}, so a new vote cannot be created or broadcast",
            round.status_label
        ));
    }
    if round.vote_end_time <= now {
        return Err("the voting window ended before this vote was broadcast".to_string());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn submit_missing_shares(
    client: &Client,
    paths: &ProfilePaths,
    profile: Profile,
    round: &RoundSnapshot,
    bundle_index: u32,
    commitment: &SignedVoteCommitment,
    vc_position: u64,
    single_share: bool,
) -> Result<u32, String> {
    let server_urls = round
        .vote_servers
        .iter()
        .map(|server| server.url.clone())
        .collect::<Vec<_>>();
    if server_urls.is_empty() {
        return Err("authenticated config has no helper servers".to_string());
    }
    let target_count = share_submission_target_count(server_urls.len());
    let existing = recorded_share_deliveries(
        paths,
        profile,
        &round.round_id,
        bundle_index,
        commitment.proposal_id,
    )?;
    let now = unix_seconds()?;
    let buffer = round
        .ceremony_start_time
        .and_then(|start| last_moment_buffer_seconds(start, round.vote_end_time));
    let required = share_submission_random_bytes_required(
        commitment.share_payloads.len(),
        server_urls.len(),
        now,
        round.vote_end_time,
        buffer,
        single_share,
    );
    let mut submit_entropy = vec![0u8; required.submit_at_random_bytes];
    let mut server_entropy = vec![0u8; required.server_random_bytes];
    OsRng.fill_bytes(&mut submit_entropy);
    OsRng.fill_bytes(&mut server_entropy);
    let plans = plan_share_submissions(
        commitment.share_payloads.len(),
        &server_urls,
        now,
        round.vote_end_time,
        buffer,
        single_share,
        &submit_entropy,
        &server_entropy,
    )
    .map_err(|error| format!("plan helper share submissions failed: {error}"))?;
    let mut submitted_count = 0u32;
    for (payload, plan) in commitment.share_payloads.iter().zip(plans) {
        let share_index = payload.enc_share.share_index;
        let recorded = existing.get(&share_index);
        let mut accepted = configured_recorded_servers(recorded.map(Vec::as_slice), &server_urls);
        if accepted.len() >= target_count {
            submitted_count = submitted_count.saturating_add(1);
            continue;
        }
        let wire = VoteShareWire::from_payload(payload, Some(vc_position), plan.submit_at)
            .map_err(|error| format!("build helper share payload failed: {error}"))?;
        let body = wire
            .to_json()
            .map_err(|error| format!("serialize helper share payload failed: {error}"))?;
        let mut candidates = plan.target_servers.clone();
        for server in &server_urls {
            if !candidates.contains(server) {
                candidates.push(server.clone());
            }
        }
        let mut attempted = HashSet::new();
        let mut newly_accepted = Vec::new();
        for server in candidates {
            if accepted.len() >= target_count {
                break;
            }
            if accepted.contains(&server) || !attempted.insert(server.clone()) {
                continue;
            }
            if chain::submit_share(client, &server, &body).await.is_ok()
                && accepted.insert(server.clone())
            {
                newly_accepted.push(server);
            }
        }
        if !newly_accepted.is_empty() {
            let db = open_db(paths, profile)?;
            let recovered =
                CommittedVote::recover(&db, &round.round_id, bundle_index, commitment.proposal_id)
                    .map_err(|error| format!("recover vote for share recording failed: {error}"))?;
            if recorded.is_some() {
                recovered
                    .add_sent_servers(&db, share_index, &newly_accepted)
                    .map_err(|error| format!("update helper share submission failed: {error}"))?;
            } else {
                recovered
                    .record_share(&db, share_index, &newly_accepted, plan.submit_at)
                    .map_err(|error| format!("record helper share submission failed: {error}"))?;
            }
        }
        if accepted.len() < target_count {
            return Err(format!(
                "only {} of {} required helpers accepted share {share_index} for proposal {}; retry to resume safely",
                accepted.len(),
                target_count,
                commitment.proposal_id,
            ));
        }
        submitted_count = submitted_count.saturating_add(1);
    }
    Ok(submitted_count)
}

fn recorded_share_deliveries(
    paths: &ProfilePaths,
    profile: Profile,
    round_id: &str,
    bundle_index: u32,
    proposal_id: u32,
) -> Result<BTreeMap<u32, Vec<String>>, String> {
    let db = open_db(paths, profile)?;
    let wallet = db.wallet_id();
    let conn = db.conn();
    let mut statement = conn
        .prepare(
            "SELECT share_index, sent_to_urls FROM share_delegations
             WHERE round_id = :round_id AND wallet_id = :wallet_id
               AND bundle_index = :bundle_index AND proposal_id = :proposal_id",
        )
        .map_err(|error| format!("prepare helper share recovery query failed: {error}"))?;
    let rows = statement
        .query_map(
            named_params! {
                ":round_id": round_id,
                ":wallet_id": wallet,
                ":bundle_index": bundle_index,
                ":proposal_id": proposal_id,
            },
            |row| Ok((row.get::<_, u32>(0)?, row.get::<_, String>(1)?)),
        )
        .map_err(|error| format!("query helper share recovery state failed: {error}"))?;
    let mut deliveries = BTreeMap::new();
    for row in rows {
        let (share_index, sent_to_urls) =
            row.map_err(|error| format!("decode helper share row failed: {error}"))?;
        let sent_to_urls = serde_json::from_str(&sent_to_urls)
            .map_err(|error| format!("decode helper share server list failed: {error}"))?;
        deliveries.insert(share_index, sent_to_urls);
    }
    Ok(deliveries)
}

fn configured_recorded_servers(
    recorded: Option<&[String]>,
    configured: &[String],
) -> HashSet<String> {
    recorded
        .unwrap_or_default()
        .iter()
        .filter(|server| configured.contains(server))
        .cloned()
        .collect()
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
    fn closed_round_allows_only_submitted_votes_to_resume() {
        let mut round = crate::demo::round_snapshot();
        round.is_active = false;
        round.status_label = "Finalized".to_string();
        round.vote_end_time = 100;
        let submitted = ExistingVote {
            choice: 0,
            tx_hash: Some("submitted-hash".to_string()),
            vc_tree_position: None,
        };
        let signed_only = ExistingVote {
            choice: 0,
            tx_hash: None,
            vc_tree_position: None,
        };

        assert!(ensure_vote_can_continue(Profile::Testnet, &round, Some(&submitted), 101).is_ok());
        assert!(
            ensure_vote_can_continue(Profile::Testnet, &round, Some(&signed_only), 101).is_err()
        );
        assert!(ensure_vote_can_continue(Profile::Testnet, &round, None, 101).is_err());

        round.is_active = true;
        assert!(ensure_vote_can_continue(Profile::Testnet, &round, Some(&submitted), 101).is_ok());
        assert!(
            ensure_vote_can_continue(Profile::Testnet, &round, Some(&signed_only), 101)
                .unwrap_err()
                .contains("voting window ended")
        );
    }

    #[test]
    fn retry_reuses_persisted_helper_acceptances() {
        let root = std::env::temp_dir().join(format!(
            "custody-voter-helper-retry-{}",
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
        db.conn()
            .execute(
                "INSERT INTO share_delegations (
                    round_id, wallet_id, bundle_index, proposal_id, share_index,
                    sent_to_urls, nullifier, confirmed, submit_at, created_at
                 ) VALUES ('round', ?1, 0, 1, 0, '[\"https://helper-a\"]', X'00', 0, 1, 1)",
                [&wallet],
            )
            .unwrap();
        drop(db);

        let deliveries =
            recorded_share_deliveries(&paths, Profile::Testnet, "round", 0, 1).unwrap();
        let configured = vec![
            "https://helper-a".to_string(),
            "https://helper-b".to_string(),
            "https://helper-c".to_string(),
        ];
        let mut accepted =
            configured_recorded_servers(deliveries.get(&0).map(Vec::as_slice), &configured);
        assert_eq!(accepted, HashSet::from(["https://helper-a".to_string()]));

        accepted.insert("https://helper-b".to_string());
        assert_eq!(
            accepted.len(),
            share_submission_target_count(configured.len())
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn manifest_failure_rolls_back_new_capability_database_state() {
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
            &db,
            "round",
            false,
        )
        .unwrap_err();

        assert!(error.contains("replace file failed"));
        assert_eq!(db.get_bundle_count("round").unwrap(), 0);
        assert!(db.round("round").unwrap().is_none());
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
}
