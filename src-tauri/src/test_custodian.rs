use std::{
    convert::Infallible,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use bip0039::{English, Mnemonic};
use ff::PrimeField;
use pasta_curves::pallas;
use prost::Message;
use rand::rngs::OsRng;
use reqwest::{Client, Url};
use rusqlite::{Connection, OpenFlags};
use secrecy::{ExposeSecret, SecretVec};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tauri::{AppHandle, Emitter};
use tonic::{
    transport::{Channel, ClientTlsConfig, Endpoint},
    Request,
};
use uuid::Uuid;
use zcash_client_backend::{
    data_api::{
        chain::{self as wallet_chain, scan_cached_blocks},
        Account as _, AccountBirthday, WalletRead, WalletWrite,
    },
    proto::{
        compact_formats::CompactBlock,
        service::{
            self, compact_tx_streamer_client::CompactTxStreamerClient, BlockId, BlockRange,
            TreeState,
        },
    },
};
use zcash_client_sqlite::{util::SystemClock, wallet::init::init_wallet_db, AccountUuid, WalletDb};
use zcash_keys::keys::UnifiedSpendingKey;
use zcash_protocol::consensus::{BlockHeight, NetworkUpgrade, Parameters};
use zcash_voting::{
    delegate::{
        DelegationProgress, DelegationSigningRequest, DelegationSubmission,
        PrepareDelegationBundleForTargetParams, PreparedSigner, ResolveDelegationLwdParams,
    },
    export_delegation_capability, minimum_voting_eligibility_for_notes,
    phases::DelegationPhase,
    prelude::{DelegationProgressBridge, NoopProgressReporter},
    round::VotingDb,
    selection::select_snapshot_note_infos,
    wire::{DelegationSubmissionWire, VotingHotkeyTargetV1},
    BundlePolicy, DelegationCapabilityV1, HyperTransport, Network, RoundBoundVotingHotkeyTarget,
    BALLOT_DIVISOR,
};
use zeroize::{Zeroize, Zeroizing};
use zip32::{fingerprint::SeedFingerprint, AccountId as Zip32AccountId};

use crate::{
    chain,
    model::{RoundSnapshot, TestCustodianProgressEvent, TestCustodianResult},
};

const JOB_FORMAT_VERSION: u32 = 2;
const DELEGATE_VOTE_TAG: u8 = 0x02;
const ZIP32_ACCOUNT_INDEX: u32 = 0;
const SYNC_BATCH_SIZE: u64 = 300;
const MAX_MNEMONIC_BYTES: usize = 2_048;
const MAX_JOB_BYTES: u64 = 256 * 1024 * 1024;
const WALLET_BUSY_TIMEOUT: Duration = Duration::from_secs(2);
const LIGHTWALLETD_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const LIGHTWALLETD_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const LIGHTWALLETD_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

type TestWalletDb = WalletDb<Connection, Network, SystemClock, OsRng>;

#[derive(Clone)]
struct JobPaths {
    wallet_database: PathBuf,
    provider_database: PathBuf,
    manifest: PathBuf,
    wallet_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TestCustodianJob {
    format_version: u32,
    vote_chain_id: String,
    vote_round_id: String,
    snapshot_height: u64,
    wallet_birthday_height: u64,
    target_json: String,
    account_uuid: String,
    account_index: u32,
    seed_fingerprint: String,
    bundle_count: u32,
    signed_transactions: Vec<SignedDelegationTransaction>,
    capability_json: Option<String>,
    capability_digest: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SignedDelegationTransaction {
    bundle_index: u32,
    body_json: String,
    raw_tx_base64: String,
    tx_hash: String,
    broadcast_accepted: bool,
}

#[derive(Clone, Copy)]
struct JobContext<'a> {
    round: &'a RoundSnapshot,
    target_json: &'a str,
    wallet_birthday_height: u64,
    seed_fingerprint: &'a str,
}

pub struct GeneratePayloadRequest {
    pub round: RoundSnapshot,
    pub target_json: String,
    pub birthday_height: u64,
    pub mnemonic: String,
    pub lightwalletd_url: String,
}

#[derive(Clone, PartialEq, Message)]
struct MsgDelegateVoteProto {
    #[prost(bytes = "vec", tag = "1")]
    rk: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    spend_auth_sig: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    signed_note_nullifier: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    cmx_new: Vec<u8>,
    #[prost(bytes = "vec", tag = "6")]
    van_cmx: Vec<u8>,
    #[prost(bytes = "vec", repeated, tag = "7")]
    gov_nullifiers: Vec<Vec<u8>>,
    #[prost(bytes = "vec", tag = "8")]
    proof: Vec<u8>,
    #[prost(bytes = "vec", tag = "9")]
    vote_round_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "11")]
    tx1_effects: Vec<u8>,
}

struct MemoryBlockSource {
    blocks: Vec<CompactBlock>,
}

impl wallet_chain::BlockSource for MemoryBlockSource {
    type Error = Infallible;

    fn with_blocks<F, WalletError>(
        &self,
        from_height: Option<BlockHeight>,
        limit: Option<usize>,
        mut with_block: F,
    ) -> Result<(), wallet_chain::error::Error<WalletError, Self::Error>>
    where
        F: FnMut(CompactBlock) -> Result<(), wallet_chain::error::Error<WalletError, Self::Error>>,
    {
        let start = from_height.map(u32::from).unwrap_or(0);
        for block in self
            .blocks
            .iter()
            .filter(|block| block.height >= u64::from(start))
            .take(limit.unwrap_or(usize::MAX))
        {
            with_block(block.clone())?;
        }
        Ok(())
    }
}

pub async fn generate_payload(
    app: AppHandle,
    client: Client,
    app_data_dir: PathBuf,
    request: GeneratePayloadRequest,
) -> Result<TestCustodianResult, String> {
    let GeneratePayloadRequest {
        round,
        target_json,
        birthday_height,
        mnemonic,
        lightwalletd_url,
    } = request;
    validate_testnet_context(&round, &target_json)?;
    let snapshot_height = round.snapshot_height;
    validate_wallet_birthday(birthday_height, snapshot_height)?;
    let lightwalletd_url = validate_lightwalletd_url(&lightwalletd_url)?;
    if mnemonic.len() > MAX_MNEMONIC_BYTES {
        return Err(format!(
            "mnemonic exceeds the {MAX_MNEMONIC_BYTES}-byte test harness limit"
        ));
    }
    let seed =
        tauri::async_runtime::spawn_blocking(move || mnemonic_to_seed(Zeroizing::new(mnemonic)))
            .await
            .map_err(|error| format!("test wallet seed task failed: {error}"))??;
    let seed_fingerprint = seed_fingerprint_hex(&seed)?;
    let paths = job_paths(
        &app_data_dir,
        &round,
        &target_json,
        birthday_height,
        &seed_fingerprint,
    );
    let context_round = round.clone();
    let context_target = target_json.clone();
    let context_fingerprint = seed_fingerprint.clone();
    let resume_paths = paths.clone();

    if let Some(job) = tauri::async_runtime::spawn_blocking(move || {
        recover_fully_signed_job(
            &resume_paths,
            JobContext {
                round: &context_round,
                target_json: &context_target,
                wallet_birthday_height: birthday_height,
                seed_fingerprint: &context_fingerprint,
            },
        )
    })
    .await
    .map_err(|error| format!("test custodian recovery task failed: {error}"))??
    {
        if job
            .signed_transactions
            .iter()
            .any(|transaction| !transaction.broadcast_accepted)
        {
            ensure_active_round(&round)?;
        }
        return broadcast_job(app, client, paths, round, job).await;
    }

    ensure_active_round(&round)?;
    emit_progress(
        &app,
        &round.round_id,
        "wallet_recovery",
        None,
        None,
        Some(0.0),
        "Creating an isolated Testnet wallet for ZIP 32 account 0",
    );
    let mut lightwalletd = open_lightwalletd(&lightwalletd_url).await?;
    let prior_height = birthday_height - 1;
    let birthday_tree_state = fetch_tree_state(&mut lightwalletd, prior_height).await?;
    let initialize_paths = paths.clone();
    let initialize_fingerprint = seed_fingerprint.clone();
    let (seed, account_uuid) = tauri::async_runtime::spawn_blocking(move || {
        initialize_or_validate_wallet(
            &initialize_paths,
            birthday_height,
            snapshot_height,
            &initialize_fingerprint,
            birthday_tree_state,
            seed,
        )
    })
    .await
    .map_err(|error| format!("test wallet initialization task failed: {error}"))??;

    sync_wallet_to_snapshot(
        &app,
        &mut lightwalletd,
        &paths.wallet_database,
        &round.round_id,
        birthday_height,
        snapshot_height,
    )
    .await?;
    drop(lightwalletd);

    let preflight_paths = paths.clone();
    let preflight_round = round.clone();
    let preflight_target = target_json.clone();
    let preflight_fingerprint = seed_fingerprint.clone();
    let preflight_account = account_uuid.clone();
    let seed = tauri::async_runtime::spawn_blocking(move || {
        preflight_incomplete_job(
            &preflight_paths,
            JobContext {
                round: &preflight_round,
                target_json: &preflight_target,
                wallet_birthday_height: birthday_height,
                seed_fingerprint: &preflight_fingerprint,
            },
            &preflight_account,
            seed,
        )
    })
    .await
    .map_err(|error| format!("test custodian wallet preflight task failed: {error}"))??;

    emit_progress(
        &app,
        &round.round_id,
        "services",
        None,
        None,
        None,
        "Authenticating Testnet services and matching the PIR snapshot",
    );
    let environment = chain::resolve_environment(&client, crate::model::Profile::Testnet).await?;
    let pir_endpoint =
        chain::resolve_pir_endpoint(&client, &environment, round.snapshot_height).await?;
    let lwd = zcash_voting::delegate::gather_delegation_lwd_inputs(ResolveDelegationLwdParams {
        lightwalletd_url: &lightwalletd_url,
        network: Network::Testnet,
        round_params: round.params.clone(),
        round_name: &round.title,
    })
    .await
    .map_err(|error| format!("load Testnet delegation anchor failed: {error}"))?;

    let build_paths = paths.clone();
    let build_round = round.clone();
    let build_target = target_json.clone();
    let build_fingerprint = seed_fingerprint.clone();
    let pir_layout = environment.pir_layout;
    let progress_app = app.clone();
    let job = tauri::async_runtime::spawn_blocking(move || {
        build_signed_job(
            &progress_app,
            &build_paths,
            JobContext {
                round: &build_round,
                target_json: &build_target,
                wallet_birthday_height: birthday_height,
                seed_fingerprint: &build_fingerprint,
            },
            seed,
            lwd,
            pir_layout,
            &pir_endpoint,
        )
    })
    .await
    .map_err(|error| format!("test custodian proof task failed: {error}"))??;

    broadcast_job(app, client, paths, round, job).await
}

fn preflight_incomplete_job(
    paths: &JobPaths,
    context: JobContext<'_>,
    account_uuid: &str,
    seed: SecretVec<u8>,
) -> Result<SecretVec<u8>, String> {
    let wallet = open_wallet_readonly(&paths.wallet_database)?;
    let account_id = parse_account_uuid(account_uuid)?;
    let matches = wallet
        .validate_seed(account_id, &seed)
        .map_err(|error| format!("validate recovered wallet seed failed: {error}"))?;
    if !matches {
        return Err("mnemonic does not control recovered ZIP 32 account 0".to_string());
    }
    let scanned_height =
        fully_scanned_height(&wallet)?.unwrap_or(context.wallet_birthday_height - 1);
    if scanned_height < context.round.snapshot_height {
        return Err(format!(
            "test wallet is synced to {scanned_height}, but this round needs snapshot height {}",
            context.round.snapshot_height
        ));
    }
    let notes = select_snapshot_note_infos(&wallet, account_uuid, context.round.snapshot_height)
        .map_err(|error| format!("select snapshot-eligible test wallet notes failed: {error}"))?;
    let eligibility = minimum_voting_eligibility_for_notes(&notes, BundlePolicy::default())
        .map_err(|error| format!("check test wallet voting eligibility failed: {error}"))?;
    if !eligibility.is_eligible() {
        return Err(format!(
            "selected account has {} zatoshi of quantized voting weight at the snapshot; at least {BALLOT_DIVISOR} is required",
            eligibility.eligible_weight
        ));
    }
    let keys = zcash_voting::delegate::load_account_keys(&wallet, account_uuid)
        .map_err(|error| format!("load recovered test wallet account keys failed: {error}"))?;
    if keys.account_index != ZIP32_ACCOUNT_INDEX {
        return Err(format!(
            "recovered wallet uses ZIP 32 account {}, expected account 0",
            keys.account_index
        ));
    }
    let fingerprint = hex::encode(keys.seed_fingerprint);
    if fingerprint != context.seed_fingerprint {
        return Err("recovered wallet seed fingerprint changed unexpectedly".to_string());
    }
    let job = match read_job(paths)? {
        Some(job) => {
            validate_job(&job, context)?;
            if job.seed_fingerprint != fingerprint {
                return Err(
                    "selected wallet seed conflicts with the recoverable test job".to_string(),
                );
            }
            if job.account_uuid != account_uuid {
                return Err("recovered account conflicts with the existing custody job".to_string());
            }
            job
        }
        None => TestCustodianJob {
            format_version: JOB_FORMAT_VERSION,
            vote_chain_id: context.round.chain_id.clone(),
            vote_round_id: context.round.round_id.clone(),
            snapshot_height: context.round.snapshot_height,
            wallet_birthday_height: context.wallet_birthday_height,
            target_json: context.target_json.to_string(),
            account_uuid: account_uuid.to_string(),
            account_index: ZIP32_ACCOUNT_INDEX,
            seed_fingerprint: fingerprint,
            bundle_count: 0,
            signed_transactions: Vec::new(),
            capability_json: None,
            capability_digest: None,
        },
    };
    validate_job(&job, context)?;
    write_job(paths, &job)?;
    Ok(seed)
}

#[allow(clippy::too_many_arguments)]
fn build_signed_job(
    app: &AppHandle,
    paths: &JobPaths,
    context: JobContext<'_>,
    seed: SecretVec<u8>,
    lwd: zcash_voting::delegate::DelegationLwdInputs,
    pir_layout: zcash_voting::config::PirLayout,
    pir_endpoint: &str,
) -> Result<TestCustodianJob, String> {
    let target = validated_target(context.round, context.target_json)?;
    let mut job = read_job(paths)?.ok_or_else(|| "test custodian job is missing".to_string())?;
    validate_job(&job, context)?;
    let account_uuid = job.account_uuid.clone();
    let provider_db = open_provider_db(paths)?;
    let wallet = open_wallet_database(&paths.wallet_database)?;

    let mut first_prepared = None;
    if job.bundle_count == 0 {
        emit_progress(
            app,
            &context.round.round_id,
            "selecting_notes",
            Some(0),
            None,
            None,
            "Selecting snapshot-eligible notes and fixing the bundle plan",
        );
        let prepared = zcash_voting::delegate::prepare_delegation_bundle_for_target(
            &provider_db,
            &wallet,
            PrepareDelegationBundleForTargetParams {
                lwd: lwd.clone(),
                session_json: None,
                account_uuid: &account_uuid,
                voting_target: &target,
                bundle_index: 0,
                bundle_policy: BundlePolicy::default(),
            },
        )
        .map_err(|error| format!("prepare first custody delegation bundle failed: {error}"))?;
        if prepared.layout.bundle_count == 0 {
            return Err("delegation bundle planner returned no eligible bundles".to_string());
        }
        job.bundle_count = prepared.layout.bundle_count;
        write_job(paths, &job)?;
        first_prepared = Some(prepared);
    }

    let first_missing = u32::try_from(job.signed_transactions.len())
        .map_err(|_| "signed test transaction count exceeds u32".to_string())?;
    let mut pir_client = None;
    for bundle_index in first_missing..job.bundle_count {
        emit_progress(
            app,
            &context.round.round_id,
            "preparing",
            Some(bundle_index),
            Some(job.bundle_count),
            None,
            &format!(
                "Preparing delegation bundle {} of {}",
                bundle_index + 1,
                job.bundle_count
            ),
        );
        let prepared = match first_prepared.take() {
            Some(prepared) if bundle_index == 0 => prepared,
            _ => zcash_voting::delegate::prepare_delegation_bundle_for_target(
                &provider_db,
                &wallet,
                PrepareDelegationBundleForTargetParams {
                    lwd: lwd.clone(),
                    session_json: None,
                    account_uuid: &account_uuid,
                    voting_target: &target,
                    bundle_index,
                    bundle_policy: BundlePolicy::default(),
                },
            )
            .map_err(|error| {
                format!("prepare custody delegation bundle {bundle_index} failed: {error}")
            })?,
        };
        if prepared.layout.bundle_count != job.bundle_count {
            return Err("test wallet bundle plan changed while the job was running".to_string());
        }
        let phase = provider_db
            .delegation_phase(&context.round.round_id, bundle_index)
            .map_err(|error| format!("load delegation recovery phase failed: {error}"))?;
        let pczt_bytes = match phase {
            DelegationPhase::Prepared => {
                prepared
                    .setup(&provider_db, &NoopProgressReporter)
                    .map_err(|error| format!("build custody delegation PCZT failed: {error}"))?
                    .pczt_bytes
            }
            DelegationPhase::PcztBuilt
            | DelegationPhase::Proved
            | DelegationPhase::Submitted
            | DelegationPhase::Confirmed => Vec::new(),
            _ => return Err("unsupported delegation recovery phase".to_string()),
        };

        let phase = provider_db
            .delegation_phase(&context.round.round_id, bundle_index)
            .map_err(|error| format!("reload delegation recovery phase failed: {error}"))?;
        if matches!(
            phase,
            DelegationPhase::Prepared | DelegationPhase::PcztBuilt
        ) {
            emit_progress(
                app,
                &context.round.round_id,
                "pir_precompute",
                Some(bundle_index),
                Some(job.bundle_count),
                None,
                "Privately fetching the nullifier proof inputs",
            );
            if pir_client.is_none() {
                pir_client = Some(
                    zcash_voting::connect_pir_blocking(
                        pir_layout,
                        pir_endpoint,
                        Arc::new(HyperTransport::new()),
                    )
                    .map_err(|error| {
                        format!("connect to authenticated PIR service failed: {error}")
                    })?,
                );
            }
            let client = pir_client
                .as_ref()
                .expect("PIR client was initialized for an unproved bundle");
            prepared
                .precompute(&provider_db, &wallet, client)
                .map_err(|error| format!("precompute custody delegation proof failed: {error}"))?;
            let progress_app = app.clone();
            let progress_round = context.round.round_id.clone();
            let bundle_count = job.bundle_count;
            let reporter = DelegationProgressBridge::new(move |progress| {
                let (phase, fraction, message) = match progress {
                    DelegationProgress::ProofStarting => (
                        "proving",
                        Some(0.0),
                        "Generating the custody delegation proof",
                    ),
                    DelegationProgress::ProofProgress(value) => (
                        "proving",
                        Some(value),
                        "Generating the custody delegation proof",
                    ),
                    DelegationProgress::ProofComplete => {
                        ("proving", Some(1.0), "Custody delegation proof complete")
                    }
                    _ => return,
                };
                emit_progress(
                    &progress_app,
                    &progress_round,
                    phase,
                    Some(bundle_index),
                    Some(bundle_count),
                    fraction,
                    message,
                );
            });
            prepared
                .prove(&provider_db, client, &reporter)
                .map_err(|error| format!("generate custody delegation proof failed: {error}"))?;
        }

        emit_progress(
            app,
            &context.round.round_id,
            "signing",
            Some(bundle_index),
            Some(job.bundle_count),
            None,
            "Signing the delegation with the transient test wallet seed",
        );
        let request = prepared
            .signing_request(&provider_db)
            .map_err(|error| format!("load custody delegation signing request failed: {error}"))?;
        let (signature, sighash) = sign_delegation_request(&seed, request)?;
        let signed = prepared
            .signed_bundle(
                &provider_db,
                pczt_bytes,
                PreparedSigner::signature(signature, sighash),
            )
            .map_err(|error| format!("assemble signed custody delegation failed: {error}"))?;
        if signed.bundle_index != bundle_index || signed.bundle_count != job.bundle_count {
            return Err(
                "signed delegation metadata conflicts with the fixed bundle plan".to_string(),
            );
        }
        let entry = transaction_entry(bundle_index, &signed.submission)?;
        validate_transaction_entry(&entry, &context.round.round_id)?;
        job.signed_transactions.push(entry);
        write_job(paths, &job)?;
    }
    drop(wallet);
    drop(pir_client);

    finalize_capability(paths, context, &target, &provider_db, &mut job)?;
    drop(provider_db);
    remove_wallet_database(paths)?;
    Ok(job)
}

fn recover_fully_signed_job(
    paths: &JobPaths,
    context: JobContext<'_>,
) -> Result<Option<TestCustodianJob>, String> {
    let Some(mut job) = read_job(paths)? else {
        return Ok(None);
    };
    validate_job(&job, context)?;
    if job.bundle_count == 0
        || job.signed_transactions.len()
            != usize::try_from(job.bundle_count)
                .map_err(|_| "bundle count exceeds usize".to_string())?
    {
        return Ok(None);
    }
    let target = validated_target(context.round, context.target_json)?;
    let provider_db = open_provider_db(paths)?;
    finalize_capability(paths, context, &target, &provider_db, &mut job)?;
    drop(provider_db);
    remove_wallet_database(paths)?;
    Ok(Some(job))
}

async fn broadcast_job(
    app: AppHandle,
    client: Client,
    paths: JobPaths,
    round: RoundSnapshot,
    mut job: TestCustodianJob,
) -> Result<TestCustodianResult, String> {
    for index in 0..job.signed_transactions.len() {
        if job.signed_transactions[index].broadcast_accepted {
            continue;
        }
        let entry = job.signed_transactions[index].clone();
        emit_progress(
            &app,
            &round.round_id,
            "broadcasting",
            Some(entry.bundle_index),
            Some(job.bundle_count),
            None,
            &format!(
                "Broadcasting delegation bundle {} of {} to Testnet",
                entry.bundle_index + 1,
                job.bundle_count
            ),
        );
        let already_committed =
            match chain::get_tx_confirmation(&client, &round.vote_servers, &entry.tx_hash).await {
                Ok(Some(confirmation)) if confirmation.code == 0 => true,
                Ok(Some(confirmation)) => {
                    return Err(if confirmation.log.trim().is_empty() {
                        format!(
                            "delegation transaction {} committed with code {}",
                            entry.tx_hash, confirmation.code
                        )
                    } else {
                        confirmation.log
                    });
                }
                Ok(None) | Err(_) => false,
            };
        if !already_committed {
            let result =
                chain::submit_delegation(&client, &round.vote_servers, &entry.body_json).await?;
            if !result.tx_hash.eq_ignore_ascii_case(&entry.tx_hash) {
                return Err(format!(
                    "vote chain returned transaction hash {}, expected {}",
                    result.tx_hash, entry.tx_hash
                ));
            }
        }
        let mark_paths = paths.clone();
        let mark_round = round.clone();
        let mark_target = job.target_json.clone();
        let mark_birthday = job.wallet_birthday_height;
        let mark_fingerprint = job.seed_fingerprint.clone();
        let mark_hash = entry.tx_hash.clone();
        tauri::async_runtime::spawn_blocking(move || {
            mark_broadcast_accepted(
                &mark_paths,
                JobContext {
                    round: &mark_round,
                    target_json: &mark_target,
                    wallet_birthday_height: mark_birthday,
                    seed_fingerprint: &mark_fingerprint,
                },
                entry.bundle_index,
                &mark_hash,
            )
        })
        .await
        .map_err(|error| format!("persist test custodian broadcast task failed: {error}"))??;
        job.signed_transactions[index].broadcast_accepted = true;
    }
    emit_progress(
        &app,
        &round.round_id,
        "payload_ready",
        None,
        Some(job.bundle_count),
        Some(1.0),
        "Canonical custody payload ready for customer import",
    );
    result_from_job(&job)
}

fn finalize_capability(
    paths: &JobPaths,
    context: JobContext<'_>,
    target: &RoundBoundVotingHotkeyTarget,
    provider_db: &VotingDb,
    job: &mut TestCustodianJob,
) -> Result<(), String> {
    validate_job(job, context)?;
    let expected_count = usize::try_from(job.bundle_count)
        .map_err(|_| "test custodian bundle count exceeds usize".to_string())?;
    if job.bundle_count == 0 || job.signed_transactions.len() != expected_count {
        return Err("cannot export a custody capability before every bundle is signed".to_string());
    }
    let raw_transactions = job
        .signed_transactions
        .iter()
        .map(|entry| {
            BASE64_STANDARD
                .decode(entry.raw_tx_base64.as_bytes())
                .map_err(|error| format!("decode persisted signed transaction failed: {error}"))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let exported = export_delegation_capability(provider_db, target, &raw_transactions)
        .map_err(|error| format!("export canonical custody capability failed: {error}"))?;
    let (canonical_json, digest) = exported.into_parts();
    let canonical_json = String::from_utf8(canonical_json)
        .map_err(|_| "canonical custody capability is not UTF-8".to_string())?;
    let digest = digest.to_hex();
    if job
        .capability_json
        .as_ref()
        .is_some_and(|existing| existing != &canonical_json)
        || job
            .capability_digest
            .as_ref()
            .is_some_and(|existing| existing != &digest)
    {
        return Err(
            "persisted custody capability conflicts with recovered provider state".to_string(),
        );
    }
    DelegationCapabilityV1::from_json(canonical_json.as_bytes())
        .map_err(|error| format!("validate exported custody capability failed: {error}"))?;
    job.capability_json = Some(canonical_json);
    job.capability_digest = Some(digest);
    write_job(paths, job)
}

fn mark_broadcast_accepted(
    paths: &JobPaths,
    context: JobContext<'_>,
    bundle_index: u32,
    tx_hash: &str,
) -> Result<(), String> {
    let mut job = read_job(paths)?.ok_or_else(|| "test custodian job is missing".to_string())?;
    validate_job(&job, context)?;
    let entry = job
        .signed_transactions
        .get_mut(
            usize::try_from(bundle_index).map_err(|_| "bundle index exceeds usize".to_string())?,
        )
        .ok_or_else(|| format!("test custodian bundle {bundle_index} is missing"))?;
    if entry.tx_hash != tx_hash {
        return Err("broadcast transaction hash conflicts with persisted job state".to_string());
    }
    let provider_db = open_provider_db(paths)?;
    zcash_voting::delegate::record_submission(
        &provider_db,
        &context.round.round_id,
        bundle_index,
        tx_hash,
    )
    .map_err(|error| format!("record test delegation submission failed: {error}"))?;
    entry.broadcast_accepted = true;
    write_job(paths, &job)
}

fn result_from_job(job: &TestCustodianJob) -> Result<TestCustodianResult, String> {
    let capability_json = job
        .capability_json
        .clone()
        .ok_or_else(|| "test custody capability is not ready".to_string())?;
    let digest = job
        .capability_digest
        .clone()
        .ok_or_else(|| "test custody capability digest is not ready".to_string())?;
    let submitted_bundle_count = u32::try_from(
        job.signed_transactions
            .iter()
            .filter(|entry| entry.broadcast_accepted)
            .count(),
    )
    .map_err(|_| "submitted bundle count exceeds u32".to_string())?;
    Ok(TestCustodianResult {
        capability_json,
        digest,
        bundle_count: job.bundle_count,
        submitted_bundle_count,
        delegation_transaction_hashes: job
            .signed_transactions
            .iter()
            .map(|entry| entry.tx_hash.clone())
            .collect(),
    })
}

fn transaction_entry(
    bundle_index: u32,
    submission: &DelegationSubmission,
) -> Result<SignedDelegationTransaction, String> {
    let body_json = submission
        .to_wire_json()
        .map_err(|error| format!("encode delegation request JSON failed: {error}"))?;
    let raw_tx = raw_delegation_tx(submission)?;
    let tx_hash = hex::encode(Sha256::digest(&raw_tx));
    Ok(SignedDelegationTransaction {
        bundle_index,
        body_json,
        raw_tx_base64: BASE64_STANDARD.encode(raw_tx),
        tx_hash,
        broadcast_accepted: false,
    })
}

fn raw_delegation_tx(submission: &DelegationSubmission) -> Result<Vec<u8>, String> {
    let message = MsgDelegateVoteProto {
        rk: submission.rk.to_vec(),
        spend_auth_sig: submission.spend_auth_sig.to_vec(),
        signed_note_nullifier: submission.nf_signed.to_vec(),
        cmx_new: submission.cmx_new.to_vec(),
        van_cmx: submission.gov_comm.to_vec(),
        gov_nullifiers: submission
            .gov_nullifiers
            .iter()
            .map(|nullifier| nullifier.to_vec())
            .collect(),
        proof: submission.proof.clone(),
        vote_round_id: hex::decode(&submission.vote_round_id)
            .map_err(|error| format!("decode delegation round id failed: {error}"))?,
        tx1_effects: submission.tx1_effects.clone(),
    };
    encode_raw_delegation_message(&message)
}

fn encode_raw_delegation_message(message: &MsgDelegateVoteProto) -> Result<Vec<u8>, String> {
    let mut raw = Vec::with_capacity(1 + message.encoded_len());
    raw.push(DELEGATE_VOTE_TAG);
    message
        .encode(&mut raw)
        .map_err(|error| format!("encode raw delegation transaction failed: {error}"))?;
    Ok(raw)
}

fn validate_transaction_entry(
    entry: &SignedDelegationTransaction,
    expected_round_id: &str,
) -> Result<(), String> {
    if entry.tx_hash.len() != 64
        || !entry
            .tx_hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("persisted delegation transaction hash is not lowercase SHA-256".to_string());
    }
    let wire: DelegationSubmissionWire = serde_json::from_str(&entry.body_json)
        .map_err(|error| format!("decode persisted delegation request failed: {error}"))?;
    if wire
        .to_json()
        .map_err(|error| format!("re-encode persisted delegation request failed: {error}"))?
        != entry.body_json
    {
        return Err("persisted delegation request JSON is not canonical".to_string());
    }
    let message = MsgDelegateVoteProto {
        rk: decode_wire_bytes(&wire.rk, "rk")?,
        spend_auth_sig: decode_wire_bytes(&wire.spend_auth_sig, "spend_auth_sig")?,
        signed_note_nullifier: decode_wire_bytes(&wire.nf_signed, "signed_note_nullifier")?,
        cmx_new: decode_wire_bytes(&wire.cmx_new, "cmx_new")?,
        van_cmx: decode_wire_bytes(&wire.gov_comm, "van_cmx")?,
        gov_nullifiers: wire
            .gov_nullifiers
            .iter()
            .map(|value| decode_wire_bytes(value, "gov_nullifier"))
            .collect::<Result<Vec<_>, String>>()?,
        proof: decode_wire_bytes(&wire.proof, "proof")?,
        vote_round_id: decode_wire_bytes(&wire.vote_round_id, "vote_round_id")?,
        tx1_effects: decode_wire_bytes(&wire.tx1_effects, "tx1_effects")?,
    };
    if hex::encode(&message.vote_round_id) != expected_round_id {
        return Err("persisted delegation request belongs to a different round".to_string());
    }
    let raw = encode_raw_delegation_message(&message)?;
    let persisted_raw = BASE64_STANDARD
        .decode(entry.raw_tx_base64.as_bytes())
        .map_err(|error| format!("decode persisted raw delegation transaction failed: {error}"))?;
    if raw != persisted_raw {
        return Err("persisted delegation JSON and raw transaction bytes conflict".to_string());
    }
    if hex::encode(Sha256::digest(&raw)) != entry.tx_hash {
        return Err("persisted delegation transaction hash does not match its bytes".to_string());
    }
    Ok(())
}

fn decode_wire_bytes(value: &str, field: &str) -> Result<Vec<u8>, String> {
    BASE64_STANDARD
        .decode(value.as_bytes())
        .map_err(|error| format!("decode delegation {field} failed: {error}"))
}

fn sign_delegation_request(
    seed: &SecretVec<u8>,
    request: DelegationSigningRequest,
) -> Result<([u8; 64], [u8; 32]), String> {
    let seed = seed.expose_secret();
    let fingerprint = SeedFingerprint::from_seed(seed)
        .ok_or_else(|| "wallet seed length is invalid for ZIP-32".to_string())?;
    if fingerprint.to_bytes() != request.seed_fingerprint {
        return Err("wallet seed fingerprint does not match the signing request".to_string());
    }
    if request.account_index != ZIP32_ACCOUNT_INDEX {
        return Err(format!(
            "delegation signing request uses ZIP 32 account {}, expected account 0",
            request.account_index
        ));
    }
    let account = Zip32AccountId::try_from(ZIP32_ACCOUNT_INDEX)
        .map_err(|_| "ZIP 32 account 0 is invalid".to_string())?;
    let usk = UnifiedSpendingKey::from_seed(&request.network, seed, account)
        .map_err(|error| format!("derive test wallet spending key failed: {error}"))?;
    let ask = orchard::keys::SpendAuthorizingKey::from(usk.orchard());
    let alpha = Option::<pallas::Scalar>::from(pallas::Scalar::from_repr(request.alpha))
        .ok_or_else(|| "delegation randomizer is not a canonical Pallas scalar".to_string())?;
    let randomized = ask.randomize(&alpha);
    let signature = randomized.sign(OsRng, &request.sighash);
    Ok(((&signature).into(), request.sighash))
}

fn mnemonic_to_seed(mnemonic: Zeroizing<String>) -> Result<SecretVec<u8>, String> {
    let word_count = mnemonic.split_whitespace().count();
    if !matches!(word_count, 12 | 15 | 18 | 21 | 24) {
        return Err("mnemonic must contain 12, 15, 18, 21, or 24 words".to_string());
    }
    let parsed = Mnemonic::<English>::from_phrase(mnemonic.as_str())
        .map_err(|error| format!("invalid mnemonic: {error}"))?;
    let mut seed = Zeroizing::new(parsed.to_seed("").to_vec());
    drop(parsed);
    let secret = SecretVec::new(seed.to_vec());
    seed.zeroize();
    Ok(secret)
}

fn seed_fingerprint_hex(seed: &SecretVec<u8>) -> Result<String, String> {
    SeedFingerprint::from_seed(seed.expose_secret())
        .map(|fingerprint| hex::encode(fingerprint.to_bytes()))
        .ok_or_else(|| "wallet seed length is invalid for ZIP 32".to_string())
}

async fn open_lightwalletd(
    lightwalletd_url: &str,
) -> Result<CompactTxStreamerClient<Channel>, String> {
    static RUSTLS_INIT: std::sync::Once = std::sync::Once::new();
    RUSTLS_INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });

    let endpoint = Endpoint::from_shared(lightwalletd_url.to_string())
        .map_err(|error| format!("invalid Testnet lightwalletd endpoint: {error}"))?
        .connect_timeout(LIGHTWALLETD_CONNECT_TIMEOUT);
    let channel = if lightwalletd_url.starts_with("https://") {
        endpoint
            .tls_config(ClientTlsConfig::new().with_webpki_roots())
            .map_err(|error| format!("configure Testnet lightwalletd TLS failed: {error}"))?
            .connect()
            .await
    } else {
        endpoint.connect().await
    }
    .map_err(|error| format!("connect to Testnet lightwalletd failed: {error}"))?;
    Ok(CompactTxStreamerClient::new(channel))
}

async fn fetch_tree_state(
    client: &mut CompactTxStreamerClient<Channel>,
    height: u64,
) -> Result<TreeState, String> {
    let response = tokio::time::timeout(
        LIGHTWALLETD_RPC_TIMEOUT,
        client.get_tree_state(Request::new(BlockId {
            height,
            hash: Vec::new(),
        })),
    )
    .await
    .map_err(|_| format!("Testnet lightwalletd tree-state request timed out at height {height}"))?
    .map_err(|error| {
        format!("Testnet lightwalletd tree-state request failed at height {height}: {error}")
    })?
    .into_inner();
    if response.height != height {
        return Err(format!(
            "Testnet lightwalletd returned tree state {}, expected {height}",
            response.height
        ));
    }
    Ok(response)
}

async fn download_compact_blocks(
    client: &mut CompactTxStreamerClient<Channel>,
    start: u64,
    end: u64,
) -> Result<Vec<CompactBlock>, String> {
    let end_height = BlockHeight::from_u32(
        u32::try_from(end).map_err(|_| format!("block height {end} exceeds u32"))?,
    );
    let pool_types = if Network::Testnet.is_nu_active(NetworkUpgrade::Nu6_3, end_height) {
        vec![
            service::PoolType::Sapling as i32,
            service::PoolType::Orchard as i32,
            service::PoolType::Ironwood as i32,
        ]
    } else {
        Vec::new()
    };
    let response = tokio::time::timeout(
        LIGHTWALLETD_RPC_TIMEOUT,
        client.get_block_range(Request::new(BlockRange {
            start: Some(BlockId {
                height: start,
                hash: Vec::new(),
            }),
            end: Some(BlockId {
                height: end,
                hash: Vec::new(),
            }),
            pool_types,
        })),
    )
    .await
    .map_err(|_| format!("Testnet compact-block request timed out for {start}..={end}"))?
    .map_err(|error| format!("download Testnet compact blocks {start}..={end} failed: {error}"))?;
    let mut stream = response.into_inner();
    let mut blocks = Vec::with_capacity((end - start + 1) as usize);
    loop {
        match tokio::time::timeout(LIGHTWALLETD_STREAM_IDLE_TIMEOUT, stream.message()).await {
            Ok(Ok(Some(block))) => blocks.push(block),
            Ok(Ok(None)) => break,
            Ok(Err(error)) => {
                return Err(format!(
                    "Testnet compact-block stream {start}..={end} failed: {error}"
                ))
            }
            Err(_) => {
                return Err(format!(
                    "Testnet compact-block stream {start}..={end} was idle for {} seconds",
                    LIGHTWALLETD_STREAM_IDLE_TIMEOUT.as_secs()
                ))
            }
        }
    }
    let expected = end - start + 1;
    if blocks.len() as u64 != expected
        || blocks
            .iter()
            .enumerate()
            .any(|(offset, block)| block.height != start + offset as u64)
    {
        return Err(format!(
            "Testnet lightwalletd returned a non-contiguous compact-block range for {start}..={end}"
        ));
    }
    Ok(blocks)
}

async fn sync_wallet_to_snapshot(
    app: &AppHandle,
    client: &mut CompactTxStreamerClient<Channel>,
    wallet_path: &Path,
    round_id: &str,
    birthday_height: u64,
    snapshot_height: u64,
) -> Result<(), String> {
    let total_blocks = snapshot_height - birthday_height + 1;
    loop {
        let height_path = wallet_path.to_path_buf();
        let scanned_height = tauri::async_runtime::spawn_blocking(move || {
            let wallet = open_wallet_readonly(&height_path)?;
            fully_scanned_height(&wallet)
        })
        .await
        .map_err(|error| format!("read Testnet sync progress task failed: {error}"))??
        .unwrap_or(birthday_height - 1);
        if scanned_height >= snapshot_height {
            emit_progress(
                app,
                round_id,
                "wallet_sync",
                None,
                None,
                Some(1.0),
                &format!("Recovered account 0 through snapshot height {snapshot_height}"),
            );
            return Ok(());
        }

        let start = (scanned_height + 1).max(birthday_height);
        let end = (start + SYNC_BATCH_SIZE - 1).min(snapshot_height);
        let completed_before = start - birthday_height;
        emit_progress(
            app,
            round_id,
            "wallet_sync",
            None,
            None,
            Some(completed_before as f64 / total_blocks as f64),
            &format!("Downloading Testnet blocks {start} through {end}"),
        );
        let from_state = fetch_tree_state(client, start - 1).await?;
        let blocks = download_compact_blocks(client, start, end).await?;
        let scan_path = wallet_path.to_path_buf();
        tauri::async_runtime::spawn_blocking(move || {
            scan_wallet_batch(&scan_path, start, end, from_state, blocks)
        })
        .await
        .map_err(|error| format!("Testnet wallet scan task failed: {error}"))??;

        let completed = end - birthday_height + 1;
        emit_progress(
            app,
            round_id,
            "wallet_sync",
            None,
            None,
            Some(completed as f64 / total_blocks as f64),
            &format!("Scanned account 0 through Testnet height {end}"),
        );
    }
}

fn scan_wallet_batch(
    wallet_path: &Path,
    start: u64,
    end: u64,
    from_state: TreeState,
    blocks: Vec<CompactBlock>,
) -> Result<(), String> {
    let chain_state = from_state
        .to_chain_state()
        .map_err(|error| format!("decode Testnet tree state at {} failed: {error}", start - 1))?;
    let source = MemoryBlockSource { blocks };
    let mut wallet = open_wallet_database(wallet_path)?;
    scan_cached_blocks(
        &Network::Testnet,
        &source,
        &mut wallet,
        BlockHeight::from_u32(start as u32),
        &chain_state,
        usize::try_from(end - start + 1)
            .map_err(|_| "Testnet scan batch size exceeds usize".to_string())?,
    )
    .map_err(|error| format!("scan Testnet blocks {start}..={end} failed: {error}"))?;
    Ok(())
}

fn initialize_or_validate_wallet(
    paths: &JobPaths,
    birthday_height: u64,
    snapshot_height: u64,
    expected_fingerprint: &str,
    birthday_tree_state: TreeState,
    seed: SecretVec<u8>,
) -> Result<(SecretVec<u8>, String), String> {
    if paths.manifest.exists() && !paths.wallet_database.exists() {
        return Err(
            "the recoverable custody job is incomplete, but its temporary wallet database is missing"
                .to_string(),
        );
    }
    let parent = paths
        .wallet_database
        .parent()
        .ok_or_else(|| "test wallet path has no parent".to_string())?;
    create_private_directory(parent)?;
    if paths.wallet_database.exists() {
        verify_sqlite_integrity(&paths.wallet_database, "existing recovered Testnet wallet")?;
    }
    let mut wallet = open_wallet_database(&paths.wallet_database)?;
    set_private_file_permissions(&paths.wallet_database)?;
    init_wallet_db(
        &mut wallet,
        Some(SecretVec::new(seed.expose_secret().to_vec())),
    )
    .map_err(|error| format!("initialize recovered Testnet wallet database failed: {error}"))?;

    let account_ids = wallet
        .get_account_ids()
        .map_err(|error| format!("list recovered Testnet accounts failed: {error}"))?;
    let account_id = match account_ids.as_slice() {
        [] => {
            let birthday = AccountBirthday::from_treestate(
                birthday_tree_state,
                Some(BlockHeight::from_u32(snapshot_height as u32)),
            )
            .map_err(|error| format!("construct Testnet wallet birthday failed: {error}"))?;
            if u64::from(u32::from(birthday.height())) != birthday_height {
                return Err(
                    "Testnet lightwalletd returned the wrong wallet birthday state".to_string(),
                );
            }
            wallet
                .create_account("Custody voting test", &seed, &birthday, None)
                .map_err(|error| format!("create recovered ZIP 32 account 0 failed: {error}"))?
                .0
        }
        [account_id] => *account_id,
        _ => {
            return Err(
                "the isolated Testnet recovery wallet unexpectedly contains multiple accounts"
                    .to_string(),
            )
        }
    };
    wallet
        .update_chain_tip(BlockHeight::from_u32(snapshot_height as u32))
        .map_err(|error| format!("set recovered wallet snapshot height failed: {error}"))?;
    let account = wallet
        .get_account(account_id)
        .map_err(|error| format!("load recovered ZIP 32 account 0 failed: {error}"))?
        .ok_or_else(|| "recovered ZIP 32 account 0 disappeared".to_string())?;
    if u64::from(u32::from(account.birthday_height())) != birthday_height {
        return Err(format!(
            "this recovery job was created with birthday height {}, not {birthday_height}",
            u32::from(account.birthday_height())
        ));
    }
    let matches = wallet
        .validate_seed(account_id, &seed)
        .map_err(|error| format!("validate recovered account seed failed: {error}"))?;
    if !matches {
        return Err("mnemonic does not control recovered ZIP 32 account 0".to_string());
    }
    let account_uuid = account_id.expose_uuid().to_string();
    let keys = zcash_voting::delegate::load_account_keys(&wallet, &account_uuid)
        .map_err(|error| format!("load recovered ZIP 32 account 0 keys failed: {error}"))?;
    if keys.account_index != ZIP32_ACCOUNT_INDEX {
        return Err(format!(
            "recovered wallet created ZIP 32 account {}, expected account 0",
            keys.account_index
        ));
    }
    if hex::encode(keys.seed_fingerprint) != expected_fingerprint {
        return Err("recovered wallet seed fingerprint does not match the mnemonic".to_string());
    }
    drop(wallet);
    verify_sqlite_integrity(&paths.wallet_database, "recovered Testnet wallet")?;
    Ok((seed, account_uuid))
}

fn remove_wallet_database(paths: &JobPaths) -> Result<(), String> {
    for path in [
        paths.wallet_database.clone(),
        PathBuf::from(format!("{}-wal", paths.wallet_database.display())),
        PathBuf::from(format!("{}-shm", paths.wallet_database.display())),
    ] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "remove temporary recovered wallet database {} failed: {error}",
                    path.display()
                ));
            }
        }
    }
    Ok(())
}

fn verify_sqlite_integrity(path: &Path, label: &str) -> Result<(), String> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| format!("open {label} failed: {error}"))?;
    let result: String = connection
        .query_row("PRAGMA quick_check(1)", [], |row| row.get(0))
        .map_err(|error| format!("check {label} integrity failed: {error}"))?;
    if result != "ok" {
        return Err(format!("{label} failed SQLite integrity check: {result}"));
    }
    Ok(())
}

fn open_wallet_readonly(path: &Path) -> Result<TestWalletDb, String> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| format!("open test wallet database read-only failed: {error}"))?;
    connection
        .busy_timeout(WALLET_BUSY_TIMEOUT)
        .map_err(|error| format!("configure test wallet timeout failed: {error}"))?;
    rusqlite::vtab::array::load_module(&connection)
        .map_err(|error| format!("load SQLite array module failed: {error}"))?;
    Ok(WalletDb::from_connection(
        connection,
        Network::Testnet,
        SystemClock,
        OsRng,
    ))
}

fn fully_scanned_height(wallet: &TestWalletDb) -> Result<Option<u64>, String> {
    wallet
        .block_fully_scanned()
        .map(|metadata| metadata.map(|metadata| u64::from(u32::from(metadata.block_height()))))
        .map_err(|error| format!("read recovered Testnet wallet scan height failed: {error}"))
}

fn open_wallet_database(path: &Path) -> Result<TestWalletDb, String> {
    let connection = Connection::open(path)
        .map_err(|error| format!("open isolated Testnet recovery wallet failed: {error}"))?;
    connection
        .busy_timeout(WALLET_BUSY_TIMEOUT)
        .map_err(|error| format!("configure Testnet recovery wallet timeout failed: {error}"))?;
    rusqlite::vtab::array::load_module(&connection)
        .map_err(|error| format!("load SQLite array module failed: {error}"))?;
    Ok(WalletDb::from_connection(
        connection,
        Network::Testnet,
        SystemClock,
        OsRng,
    ))
}

fn open_provider_db(paths: &JobPaths) -> Result<VotingDb, String> {
    let parent = paths
        .provider_database
        .parent()
        .ok_or_else(|| "test custodian voting database path has no parent".to_string())?;
    create_private_directory(parent)?;
    let db = VotingDb::open_path(&paths.provider_database)
        .map_err(|error| format!("open test custodian voting database failed: {error}"))?;
    set_private_file_permissions(&paths.provider_database)?;
    db.set_wallet_id(&paths.wallet_id);
    Ok(db)
}

fn read_job(paths: &JobPaths) -> Result<Option<TestCustodianJob>, String> {
    let metadata = match fs::metadata(&paths.manifest) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("read test custodian job metadata failed: {error}")),
    };
    if metadata.len() > MAX_JOB_BYTES {
        return Err(format!("test custodian job exceeds {MAX_JOB_BYTES} bytes"));
    }
    let bytes = fs::read(&paths.manifest)
        .map_err(|error| format!("read test custodian job failed: {error}"))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| format!("decode test custodian job failed: {error}"))
}

fn write_job(paths: &JobPaths, job: &TestCustodianJob) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(job)
        .map_err(|error| format!("encode test custodian job failed: {error}"))?;
    if bytes.len() as u64 > MAX_JOB_BYTES {
        return Err(format!("test custodian job exceeds {MAX_JOB_BYTES} bytes"));
    }
    write_private_atomic(&paths.manifest, &bytes)
}

fn validate_job(job: &TestCustodianJob, context: JobContext<'_>) -> Result<(), String> {
    if job.format_version != JOB_FORMAT_VERSION
        || job.vote_chain_id != context.round.chain_id
        || job.vote_round_id != context.round.round_id
        || job.snapshot_height != context.round.snapshot_height
        || job.wallet_birthday_height != context.wallet_birthday_height
        || job.target_json != context.target_json
        || job.seed_fingerprint != context.seed_fingerprint
        || job.account_index != ZIP32_ACCOUNT_INDEX
    {
        return Err(
            "test custodian job belongs to a different wallet, target, or round".to_string(),
        );
    }
    parse_account_uuid(&job.account_uuid)?;
    if job.seed_fingerprint.len() != 64
        || !job
            .seed_fingerprint
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("test custodian job has an invalid seed fingerprint".to_string());
    }
    if job.signed_transactions.len()
        > usize::try_from(job.bundle_count)
            .map_err(|_| "test custodian bundle count exceeds usize".to_string())?
    {
        return Err("test custodian job has more transactions than bundles".to_string());
    }
    for (index, entry) in job.signed_transactions.iter().enumerate() {
        if entry.bundle_index
            != u32::try_from(index).map_err(|_| "transaction index exceeds u32".to_string())?
        {
            return Err("test custodian transaction indices are not contiguous".to_string());
        }
        validate_transaction_entry(entry, &context.round.round_id)?;
    }
    match (&job.capability_json, &job.capability_digest) {
        (Some(json), Some(digest)) => {
            if job.signed_transactions.len()
                != usize::try_from(job.bundle_count)
                    .map_err(|_| "test custodian bundle count exceeds usize".to_string())?
                || job.bundle_count == 0
            {
                return Err(
                    "test custody capability exists before every bundle is signed".to_string(),
                );
            }
            DelegationCapabilityV1::from_json(json.as_bytes()).map_err(|error| {
                format!("validate persisted custody capability failed: {error}")
            })?;
            if hex::encode(Sha256::digest(json.as_bytes())) != *digest {
                return Err(
                    "test custody capability digest does not match its exact bytes".to_string(),
                );
            }
        }
        (None, None) => {}
        _ => {
            return Err("test custody capability and digest must be persisted together".to_string())
        }
    }
    Ok(())
}

fn validated_target(
    round: &RoundSnapshot,
    target_json: &str,
) -> Result<RoundBoundVotingHotkeyTarget, String> {
    VotingHotkeyTargetV1::from_json(target_json)
        .and_then(|target| target.validate_for(&round.chain_id, Network::Testnet, &round.params))
        .map_err(|error| format!("validate customer target for test custodian failed: {error}"))
}

fn validate_testnet_context(round: &RoundSnapshot, target_json: &str) -> Result<(), String> {
    if round.profile != crate::model::Profile::Testnet
        || round.chain_id != crate::model::Profile::Testnet.chain_id()
        || !round.authenticated
    {
        return Err(
            "the custodian simulator is available only for authenticated Testnet rounds"
                .to_string(),
        );
    }
    validated_target(round, target_json).map(|_| ())
}

fn ensure_active_round(round: &RoundSnapshot) -> Result<(), String> {
    if round.is_active {
        Ok(())
    } else {
        Err(format!(
            "the selected Testnet round is {}, so it cannot accept a new delegation",
            round.status_label
        ))
    }
}

fn validate_wallet_birthday(birthday_height: u64, snapshot_height: u64) -> Result<(), String> {
    let sapling_activation = Network::Testnet
        .activation_height(NetworkUpgrade::Sapling)
        .map(|height| u64::from(u32::from(height)))
        .ok_or_else(|| "Testnet Sapling activation height is unavailable".to_string())?;
    if snapshot_height > u64::from(u32::MAX) {
        return Err("round snapshot height exceeds the supported Zcash height range".to_string());
    }
    if birthday_height < sapling_activation {
        return Err(format!(
            "wallet birthday must be at or after Testnet Sapling activation height {sapling_activation}"
        ));
    }
    if birthday_height > snapshot_height {
        return Err(format!(
            "wallet birthday {birthday_height} is after round snapshot {snapshot_height}"
        ));
    }
    Ok(())
}

fn validate_lightwalletd_url(value: &str) -> Result<String, String> {
    let url = Url::parse(value.trim())
        .map_err(|error| format!("invalid Testnet lightwalletd URL: {error}"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "lightwalletd URL must be an http(s) URL without credentials or a fragment".to_string(),
        );
    }
    Ok(url.to_string())
}

fn parse_account_uuid(value: &str) -> Result<AccountUuid, String> {
    Uuid::parse_str(value)
        .map(AccountUuid::from_uuid)
        .map_err(|error| format!("invalid test wallet account UUID: {error}"))
}

fn job_paths(
    app_data_dir: &Path,
    round: &RoundSnapshot,
    target_json: &str,
    birthday_height: u64,
    seed_fingerprint: &str,
) -> JobPaths {
    let mut hasher = Sha256::new();
    for part in [
        round.chain_id.as_bytes(),
        round.round_id.as_bytes(),
        target_json.as_bytes(),
        seed_fingerprint.as_bytes(),
    ] {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    hasher.update(birthday_height.to_be_bytes());
    let digest = hex::encode(hasher.finalize());
    let directory = app_data_dir
        .join("test-custodian")
        .join("testnet")
        .join(&round.round_id)
        .join(&digest);
    JobPaths {
        wallet_database: directory.join("wallet.sqlite"),
        provider_database: directory.join("provider-voting.sqlite"),
        manifest: directory.join("job.json"),
        wallet_id: format!("custody-voter-test-provider-{digest}"),
    }
}

fn write_private_atomic(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "test custodian job path has no parent".to_string())?;
    create_private_directory(parent)?;
    let temp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy(),
        Uuid::new_v4()
    ));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)
        .map_err(|error| format!("create temporary test custodian job failed: {error}"))?;
    set_private_file_permissions(&temp)?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| format!("durably write test custodian job failed: {error}"))?;
    drop(file);
    fs::rename(&temp, path).map_err(|error| {
        let _ = fs::remove_file(&temp);
        format!("activate test custodian job failed: {error}")
    })?;
    if let Ok(directory) = File::open(parent) {
        let _ = directory.sync_all();
    }
    Ok(())
}

fn create_private_directory(path: &Path) -> Result<(), String> {
    fs::create_dir_all(path)
        .map_err(|error| format!("create test custodian job directory failed: {error}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| format!("protect test custodian job directory failed: {error}"))?;
    }
    Ok(())
}

fn set_private_file_permissions(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|error| format!("protect sensitive test custodian file failed: {error}"))?;
    }
    Ok(())
}

fn emit_progress(
    app: &AppHandle,
    round_id: &str,
    phase: &str,
    bundle_index: Option<u32>,
    bundle_count: Option<u32>,
    progress: Option<f64>,
    message: &str,
) {
    let _ = app.emit(
        "custodian-progress",
        TestCustodianProgressEvent {
            round_id: round_id.to_string(),
            phase: phase.to_string(),
            bundle_index,
            bundle_count,
            progress,
            message: message.to_string(),
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_delegation_transaction_matches_vote_sdk_wire_shape() {
        let message = MsgDelegateVoteProto {
            rk: vec![1; 2],
            spend_auth_sig: vec![2; 3],
            signed_note_nullifier: vec![3; 4],
            cmx_new: vec![4; 5],
            van_cmx: vec![6; 7],
            gov_nullifiers: vec![vec![7; 2], vec![8; 2]],
            proof: vec![9; 3],
            vote_round_id: vec![10; 4],
            tx1_effects: vec![11; 5],
        };
        let raw = encode_raw_delegation_message(&message).unwrap();
        // Generated independently with vote-sdk's checked-in MsgDelegateVote
        // protobuf schema and custom transaction tag.
        assert_eq!(
            hex::encode(&raw),
            "020a02010112030202021a0403030303220504040404043207060606060606063a0207073a02080842030909094a040a0a0a0a5a050b0b0b0b0b"
        );
        assert_eq!(MsgDelegateVoteProto::decode(&raw[1..]).unwrap(), message);
        assert_eq!(hex::encode(Sha256::digest(&raw)).len(), 64);
    }

    #[test]
    fn lightwalletd_url_rejects_embedded_credentials() {
        assert!(validate_lightwalletd_url("https://testnet.example:443").is_ok());
        assert!(validate_lightwalletd_url("https://user:test@testnet.example").is_err());
    }

    #[tokio::test]
    #[ignore = "requires a public Testnet lightwalletd"]
    async fn recovers_account_zero_and_scans_real_testnet_blocks() {
        let mnemonic = Zeroizing::new(
            "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"
                .to_string(),
        );
        let seed = mnemonic_to_seed(mnemonic).unwrap();
        let fingerprint = seed_fingerprint_hex(&seed).unwrap();
        let temp = std::env::temp_dir().join(format!("custody-voter-sync-{}", Uuid::new_v4()));
        let paths = JobPaths {
            wallet_database: temp.join("wallet.sqlite"),
            provider_database: temp.join("provider.sqlite"),
            manifest: temp.join("job.json"),
            wallet_id: "test-wallet".to_string(),
        };
        // This range ends at a Stage snapshot after Testnet NU6.3, so the
        // smoke test exercises Ironwood compact blocks across two batches.
        let birthday_height = 4_245_160;
        let snapshot_height = birthday_height + SYNC_BATCH_SIZE;
        let mut client = open_lightwalletd("https://testnet.zec.rocks:443")
            .await
            .unwrap();
        let tree_state = fetch_tree_state(&mut client, birthday_height - 1)
            .await
            .unwrap();
        let (seed, account_uuid) = initialize_or_validate_wallet(
            &paths,
            birthday_height,
            snapshot_height,
            &fingerprint,
            tree_state,
            seed,
        )
        .unwrap();
        let first_end = birthday_height + SYNC_BATCH_SIZE - 1;
        for (start, end) in [
            (birthday_height, first_end),
            (first_end + 1, snapshot_height),
        ] {
            let blocks = download_compact_blocks(&mut client, start, end)
                .await
                .unwrap();
            let from_state = fetch_tree_state(&mut client, start - 1).await.unwrap();
            scan_wallet_batch(&paths.wallet_database, start, end, from_state, blocks).unwrap();
        }
        let wallet = open_wallet_readonly(&paths.wallet_database).unwrap();
        assert_eq!(
            fully_scanned_height(&wallet).unwrap(),
            Some(snapshot_height)
        );
        let account_id = parse_account_uuid(&account_uuid).unwrap();
        assert!(wallet.validate_seed(account_id, &seed).unwrap());
        let keys = zcash_voting::delegate::load_account_keys(&wallet, &account_uuid).unwrap();
        assert_eq!(keys.account_index, ZIP32_ACCOUNT_INDEX);
        drop(wallet);
        fs::remove_dir_all(temp).unwrap();
    }
}
