mod chain;
mod demo;
mod model;
mod storage;
#[cfg(debug_assertions)]
mod test_custodian;
mod voter;

use std::{collections::HashSet, fs, path::PathBuf};

#[cfg(debug_assertions)]
use model::TestCustodianResult;
use model::{
    BackupResult, CastVotesResult, ImportResult, Profile, ResetResult, RestoreResult, RoundCard,
    RoundWorkspace, TargetResult, VoteChoiceInput,
};
use reqwest::Client;
use storage::{profile_paths, read_manifest, round_progress, vote_records};
use tauri::{Manager, State};
use tokio::sync::Mutex;
use uuid::Uuid;

struct AppState {
    app_data_dir: PathBuf,
    client: Client,
    operation_lock: Mutex<()>,
    reset_token: Mutex<Option<String>>,
}

#[tauri::command]
async fn list_rounds(
    profile: Profile,
    state: State<'_, AppState>,
) -> Result<Vec<RoundCard>, String> {
    let _guard = state.operation_lock.lock().await;
    let paths = profile_paths(&state.app_data_dir, profile)?;
    let manifest = read_manifest(&paths, profile)?;
    let mut rounds = match chain::list_rounds(&state.client, profile).await {
        Ok(rounds) => rounds,
        Err(_error) if !manifest.rounds.is_empty() => Vec::new(),
        Err(error) => return Err(error),
    };
    let mut seen = rounds
        .iter()
        .map(|round| round.round_id.clone())
        .collect::<HashSet<_>>();
    for stored in manifest.rounds.values() {
        if seen.insert(stored.snapshot.round_id.clone()) {
            rounds.push(stored.snapshot.clone());
        }
    }
    let mut cards = rounds
        .into_iter()
        .map(|round| {
            let stored = manifest.rounds.contains_key(&round.round_id);
            if let Some(stored_round) = manifest.rounds.get(&round.round_id) {
                voter::validate_stored_round(stored_round, &round)?;
            }
            let progress = round_progress(
                &paths,
                profile,
                &round.round_id,
                stored,
                &round.vote_servers,
            )?;
            Ok(RoundCard {
                round,
                progress,
                stored,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    cards.sort_by(|left, right| {
        right
            .round
            .is_active
            .cmp(&left.round.is_active)
            .then_with(|| right.stored.cmp(&left.stored))
            .then_with(|| right.round.vote_end_time.cmp(&left.round.vote_end_time))
    });
    Ok(cards)
}

#[tauri::command]
async fn get_round_workspace(
    profile: Profile,
    round_id: String,
    state: State<'_, AppState>,
) -> Result<RoundWorkspace, String> {
    let _guard = state.operation_lock.lock().await;
    let paths = profile_paths(&state.app_data_dir, profile)?;
    let manifest = read_manifest(&paths, profile)?;
    let round = match chain::fetch_round(&state.client, profile, &round_id).await {
        Ok(round) => {
            if let Some(stored) = manifest.rounds.get(&round_id) {
                voter::validate_stored_round(stored, &round)?;
            }
            round
        }
        Err(network_error) => manifest
            .rounds
            .get(&round_id)
            .map(|stored| stored.snapshot.clone())
            .ok_or(network_error)?,
    };
    let stored = manifest.rounds.get(&round_id);
    Ok(RoundWorkspace {
        progress: round_progress(
            &paths,
            profile,
            &round_id,
            stored.is_some(),
            &round.vote_servers,
        )?,
        target_json: stored.map(|round| round.target_json.clone()),
        capability_digest: stored.and_then(|round| round.capability_digest.clone()),
        votes: vote_records(&paths, profile, &round_id)?,
        round,
    })
}

#[tauri::command]
async fn generate_target(
    profile: Profile,
    round_id: String,
    state: State<'_, AppState>,
) -> Result<TargetResult, String> {
    let _guard = state.operation_lock.lock().await;
    let paths = profile_paths(&state.app_data_dir, profile)?;
    let round = chain::fetch_round(&state.client, profile, &round_id).await?;
    voter::generate_target(&paths, profile, round)
}

#[tauri::command]
async fn generate_demo_capability(
    round_id: String,
    state: State<'_, AppState>,
) -> Result<String, String> {
    let _guard = state.operation_lock.lock().await;
    let paths = profile_paths(&state.app_data_dir, Profile::Demo)?;
    voter::demo_capability(&paths, &round_id)
}

#[tauri::command]
async fn import_capability(
    profile: Profile,
    round_id: String,
    capability_bytes: Vec<u8>,
    state: State<'_, AppState>,
) -> Result<ImportResult, String> {
    let _guard = state.operation_lock.lock().await;
    let paths = profile_paths(&state.app_data_dir, profile)?;
    let round = chain::fetch_round(&state.client, profile, &round_id).await?;
    voter::import_capability(&state.client, &paths, profile, round, capability_bytes).await
}

#[tauri::command]
async fn check_delegation_confirmations(
    profile: Profile,
    round_id: String,
    state: State<'_, AppState>,
) -> Result<ImportResult, String> {
    let _guard = state.operation_lock.lock().await;
    let paths = profile_paths(&state.app_data_dir, profile)?;
    let round = chain::fetch_round(&state.client, profile, &round_id).await?;
    voter::refresh_delegations(&state.client, &paths, profile, &round).await
}

#[cfg(debug_assertions)]
#[tauri::command]
async fn generate_testnet_custody_payload(
    app: tauri::AppHandle,
    round_id: String,
    birthday_height: u64,
    mnemonic: String,
    lightwalletd_url: String,
    state: State<'_, AppState>,
) -> Result<TestCustodianResult, String> {
    let _guard = state.operation_lock.lock().await;
    let profile = Profile::Testnet;
    let paths = profile_paths(&state.app_data_dir, profile)?;
    let manifest = read_manifest(&paths, profile)?;
    let stored = manifest.rounds.get(&round_id).ok_or_else(|| {
        "generate the customer target before running the test custodian".to_string()
    })?;
    let round = chain::fetch_round(&state.client, profile, &round_id).await?;
    voter::validate_stored_round(stored, &round)?;
    test_custodian::generate_payload(
        app,
        state.client.clone(),
        state.app_data_dir.clone(),
        test_custodian::GeneratePayloadRequest {
            round,
            target_json: stored.target_json.clone(),
            birthday_height,
            mnemonic,
            lightwalletd_url,
        },
    )
    .await
}

#[tauri::command]
async fn cast_votes(
    app: tauri::AppHandle,
    profile: Profile,
    round_id: String,
    choices: Vec<VoteChoiceInput>,
    state: State<'_, AppState>,
) -> Result<CastVotesResult, String> {
    let _guard = state.operation_lock.lock().await;
    let paths = profile_paths(&state.app_data_dir, profile)?;
    let round = chain::fetch_round(&state.client, profile, &round_id).await?;
    voter::cast_votes(app, state.client.clone(), paths, profile, round, choices).await
}

#[tauri::command]
async fn export_backup(
    profile: Profile,
    passphrase: String,
    state: State<'_, AppState>,
) -> Result<BackupResult, String> {
    let _guard = state.operation_lock.lock().await;
    let paths = profile_paths(&state.app_data_dir, profile)?;
    storage::export_backup(&paths, profile, passphrase)
}

#[tauri::command]
async fn restore_backup(
    profile: Profile,
    passphrase: String,
    encrypted_bytes: Vec<u8>,
    state: State<'_, AppState>,
) -> Result<RestoreResult, String> {
    let _guard = state.operation_lock.lock().await;
    let paths = profile_paths(&state.app_data_dir, profile)?;
    storage::restore_backup(&paths, profile, passphrase, encrypted_bytes)
}

#[tauri::command]
async fn prepare_reset(state: State<'_, AppState>) -> Result<String, String> {
    let token = Uuid::new_v4().to_string();
    *state.reset_token.lock().await = Some(token.clone());
    Ok(token)
}

#[tauri::command]
async fn reset_all_data(
    confirmation_token: String,
    state: State<'_, AppState>,
) -> Result<ResetResult, String> {
    let _guard = state.operation_lock.lock().await;
    if state.reset_token.lock().await.as_deref() != Some(confirmation_token.as_str()) {
        return Err("reset confirmation is missing or expired".to_string());
    }
    let result = storage::reset_all_data(&state.app_data_dir);
    if result.is_ok() {
        state.reset_token.lock().await.take();
    }
    result
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let builder = tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            let app_data_dir = app.path().app_data_dir().map_err(|error| {
                std::io::Error::other(format!("resolve app data directory failed: {error}"))
            })?;
            fs::create_dir_all(&app_data_dir)?;
            app.manage(AppState {
                app_data_dir,
                client: chain::http_client().map_err(std::io::Error::other)?,
                operation_lock: Mutex::new(()),
                reset_token: Mutex::new(None),
            });
            Ok(())
        });
    #[cfg(debug_assertions)]
    let builder = builder.invoke_handler(tauri::generate_handler![
        list_rounds,
        get_round_workspace,
        generate_target,
        generate_demo_capability,
        import_capability,
        check_delegation_confirmations,
        generate_testnet_custody_payload,
        cast_votes,
        export_backup,
        restore_backup,
        prepare_reset,
        reset_all_data,
    ]);
    #[cfg(not(debug_assertions))]
    let builder = builder.invoke_handler(tauri::generate_handler![
        list_rounds,
        get_round_workspace,
        generate_target,
        generate_demo_capability,
        import_capability,
        check_delegation_confirmations,
        cast_votes,
        export_backup,
        restore_backup,
        prepare_reset,
        reset_all_data,
    ]);
    builder
        .run(tauri::generate_context!())
        .expect("error while running Zcash Custody Voter");
}
