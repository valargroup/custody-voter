use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use age::secrecy::SecretString;
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use keyring::Entry;
use rusqlite::{named_params, OptionalExtension};
use uuid::Uuid;
use zcash_voting::{round::VotingDb, VotingHotkey, BALLOT_DIVISOR};
use zeroize::{Zeroize, Zeroizing};

use crate::model::{
    BackupEnvelope, BackupHotkey, BackupResult, Profile, ProfileManifest, ResetResult,
    RestoreResult, RoundProgress, VoteRecordView, BACKUP_FORMAT_VERSION, MANIFEST_FORMAT_VERSION,
};

const KEYRING_SERVICE_PREFIX: &str = "com.valargroup.custodyvoter";
const WALLET_ID_PREFIX: &str = "custody-voter";
const MIN_BACKUP_PASSPHRASE_CHARS: usize = 12;
const MAX_BACKUP_PASSPHRASE_CHARS: usize = 1_024;
const MAX_BACKUP_BYTES: usize = 128 * 1024 * 1024;
type CapturedHotkeys = BTreeMap<String, Option<Zeroizing<Vec<u8>>>>;

#[derive(Clone, Debug)]
pub struct ProfilePaths {
    pub directory: PathBuf,
    pub database: PathBuf,
    pub manifest: PathBuf,
}

struct TemporaryDatabase(PathBuf);

impl Drop for TemporaryDatabase {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        for suffix in ["-wal", "-shm"] {
            let _ = fs::remove_file(format!("{}{}", self.0.display(), suffix));
        }
    }
}

pub fn profile_paths(app_data_dir: &Path, profile: Profile) -> Result<ProfilePaths, String> {
    let directory = app_data_dir.join(profile.slug());
    fs::create_dir_all(&directory)
        .map_err(|error| format!("create profile directory failed: {error}"))?;
    Ok(ProfilePaths {
        database: directory.join("voting.sqlite"),
        manifest: directory.join("manifest.json"),
        directory,
    })
}

pub fn wallet_id(profile: Profile) -> String {
    format!("{WALLET_ID_PREFIX}-{}", profile.slug())
}

pub fn open_db(paths: &ProfilePaths, profile: Profile) -> Result<VotingDb, String> {
    let db = VotingDb::open_path(&paths.database)
        .map_err(|error| format!("open voting database failed: {error}"))?;
    db.set_wallet_id(&wallet_id(profile));
    Ok(db)
}

pub fn read_manifest(paths: &ProfilePaths, profile: Profile) -> Result<ProfileManifest, String> {
    let bytes = match fs::read(&paths.manifest) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ProfileManifest::empty(profile));
        }
        Err(error) => return Err(format!("read profile manifest failed: {error}")),
    };
    let manifest: ProfileManifest = serde_json::from_slice(&bytes)
        .map_err(|error| format!("decode profile manifest failed: {error}"))?;
    validate_manifest(&manifest, profile)?;
    Ok(manifest)
}

pub fn write_manifest(paths: &ProfilePaths, manifest: &ProfileManifest) -> Result<(), String> {
    validate_manifest(manifest, manifest.profile)?;
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|error| format!("encode profile manifest failed: {error}"))?;
    atomic_write(&paths.manifest, &bytes)
}

fn validate_manifest(manifest: &ProfileManifest, expected_profile: Profile) -> Result<(), String> {
    if manifest.format_version != MANIFEST_FORMAT_VERSION {
        return Err(format!(
            "unsupported manifest format version {}",
            manifest.format_version
        ));
    }
    if manifest.profile != expected_profile {
        return Err("profile manifest belongs to a different network profile".to_string());
    }
    for (round_id, round) in &manifest.rounds {
        if round_id != &round.snapshot.round_id
            || round.snapshot.profile != expected_profile
            || round.snapshot.chain_id != expected_profile.chain_id()
        {
            return Err(format!(
                "manifest round {round_id} has conflicting profile context"
            ));
        }
    }
    Ok(())
}

fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "output path has no parent directory".to_string())?;
    fs::create_dir_all(parent)
        .map_err(|error| format!("create output directory failed: {error}"))?;
    let temp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy(),
        Uuid::new_v4()
    ));
    fs::write(&temp, bytes).map_err(|error| format!("write temporary file failed: {error}"))?;
    fs::rename(&temp, path).map_err(|error| {
        let _ = fs::remove_file(&temp);
        format!("replace file failed: {error}")
    })
}

fn keyring_entry(profile: Profile, round_id: &str) -> Result<Entry, String> {
    let service = format!("{KEYRING_SERVICE_PREFIX}.{}", profile.slug());
    let username = format!("round:{round_id}");
    Entry::new(&service, &username)
        .map_err(|error| format!("open operating-system keyring failed: {error}"))
}

pub fn store_hotkey(profile: Profile, round_id: &str, secret: &[u8]) -> Result<(), String> {
    keyring_entry(profile, round_id)?
        .set_secret(secret)
        .map_err(|error| format!("store voting hotkey in operating-system keyring failed: {error}"))
}

pub fn load_hotkey(profile: Profile, round_id: &str) -> Result<VotingHotkey, String> {
    let mut secret = Zeroizing::new(keyring_entry(profile, round_id)?.get_secret().map_err(
        |error| format!("load voting hotkey from operating-system keyring failed: {error}"),
    )?);
    let result = VotingHotkey::from_stored_secret(&secret, profile.network())
        .map_err(|error| format!("reconstruct stored voting hotkey failed: {error}"));
    secret.zeroize();
    result
}

pub fn delete_hotkey(profile: Profile, round_id: &str) -> Result<(), String> {
    match keyring_entry(profile, round_id)?.delete_credential() {
        Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
        Err(error) => Err(format!(
            "delete voting hotkey from operating-system keyring failed: {error}"
        )),
    }
}

/// Removes every known local voting profile and its associated Keychain hotkeys.
///
/// Hotkeys are captured before deletion so a reset failure can restore the
/// previous credentials while the corresponding manifests are still available.
pub fn reset_all_data(app_data_dir: &Path) -> Result<ResetResult, String> {
    let mut profiles = Vec::new();
    let mut removed_rounds = 0u32;
    for profile in [Profile::Mainnet, Profile::Testnet, Profile::Demo] {
        let directory = app_data_dir.join(profile.slug());
        if !directory.exists() {
            continue;
        }
        let paths = ProfilePaths {
            database: directory.join("voting.sqlite"),
            manifest: directory.join("manifest.json"),
            directory,
        };
        let manifest = read_manifest(&paths, profile)?;
        removed_rounds = removed_rounds
            .checked_add(
                u32::try_from(manifest.rounds.len())
                    .map_err(|_| "stored round count exceeds u32".to_string())?,
            )
            .ok_or_else(|| "stored round count exceeds u32".to_string())?;
        let round_ids = manifest.rounds.keys().cloned().collect::<BTreeSet<_>>();
        let hotkeys = capture_hotkeys(profile, &round_ids)?;
        profiles.push((profile, paths, hotkeys));
    }

    let keyring_reset = (|| {
        for (profile, _, hotkeys) in &profiles {
            for round_id in hotkeys.keys() {
                delete_hotkey(*profile, round_id)?;
            }
        }
        Ok::<_, String>(())
    })();
    if let Err(error) = keyring_reset {
        return Err(reset_error_with_hotkey_rollback(error, &profiles));
    }
    if app_data_dir.exists() {
        if let Err(error) = fs::remove_dir_all(app_data_dir) {
            let error = format!(
                "remove application data directory {} failed: {error}",
                app_data_dir.display()
            );
            return Err(reset_error_with_hotkey_rollback(error, &profiles));
        }
    }

    Ok(ResetResult {
        removed_profiles: u32::try_from(profiles.len())
            .map_err(|_| "stored profile count exceeds u32".to_string())?,
        removed_rounds,
    })
}

pub fn round_progress(
    paths: &ProfilePaths,
    profile: Profile,
    round_id: &str,
    target_ready: bool,
) -> Result<RoundProgress, String> {
    if !paths.database.exists() {
        return Ok(RoundProgress {
            target_ready,
            ..RoundProgress::default()
        });
    }
    let db = open_db(paths, profile)?;
    let wallet = db.wallet_id();
    let conn = db.conn();
    let bundle_stats: Option<(i64, i64, i64)> = conn
        .query_row(
            "SELECT COUNT(*),
                    COALESCE(SUM(CASE WHEN van_leaf_position IS NOT NULL THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(total_note_value), 0)
             FROM bundles
             WHERE round_id = :round_id AND wallet_id = :wallet_id",
            named_params! { ":round_id": round_id, ":wallet_id": wallet },
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|error| format!("load delegation progress failed: {error}"))?;
    let vote_stats: (i64, i64, i64) = conn
        .query_row(
            "SELECT COUNT(*),
                    COALESCE(SUM(CASE WHEN tx_hash IS NOT NULL THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN vc_tree_position IS NOT NULL THEN 1 ELSE 0 END), 0)
             FROM votes
             WHERE round_id = :round_id AND wallet_id = :wallet_id",
            named_params! { ":round_id": round_id, ":wallet_id": wallet },
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(|error| format!("load vote progress failed: {error}"))?;
    let required_share_count = {
        let mut statement = conn
            .prepare(
                "SELECT commitment_bundle_json
                 FROM votes
                 WHERE round_id = :round_id AND wallet_id = :wallet_id
                   AND commitment_bundle_json IS NOT NULL",
            )
            .map_err(|error| format!("prepare helper share progress query failed: {error}"))?;
        let rows = statement
            .query_map(
                named_params! { ":round_id": round_id, ":wallet_id": wallet },
                |row| row.get::<_, String>(0),
            )
            .map_err(|error| format!("query required helper shares failed: {error}"))?;
        let mut total = 0u32;
        for row in rows {
            let commitment_json =
                row.map_err(|error| format!("decode vote commitment state failed: {error}"))?;
            total = total
                .checked_add(encrypted_share_count(&commitment_json)?)
                .ok_or_else(|| {
                    "required helper share count is outside the supported range".to_string()
                })?;
        }
        total
    };
    let submitted_share_count: i64 = conn
        .query_row(
            "SELECT COUNT(*)
             FROM share_delegations
             WHERE round_id = :round_id AND wallet_id = :wallet_id",
            named_params! { ":round_id": round_id, ":wallet_id": wallet },
            |row| row.get(0),
        )
        .map_err(|error| format!("load submitted helper share progress failed: {error}"))?;
    let (bundle_count, confirmed_bundle_count, delegated_value) = bundle_stats.unwrap_or_default();
    Ok(RoundProgress {
        target_ready,
        capability_imported: bundle_count > 0,
        bundle_count: to_u32(bundle_count, "bundle count")?,
        confirmed_bundle_count: to_u32(confirmed_bundle_count, "confirmed bundle count")?,
        delegated_ballots: to_u64(delegated_value, "delegated value")? / BALLOT_DIVISOR,
        vote_count: to_u32(vote_stats.0, "vote count")?,
        submitted_vote_count: to_u32(vote_stats.1, "submitted vote count")?,
        confirmed_vote_count: to_u32(vote_stats.2, "confirmed vote count")?,
        required_share_count,
        submitted_share_count: to_u32(submitted_share_count, "submitted helper share count")?,
    })
}

fn encrypted_share_count(commitment_json: &str) -> Result<u32, String> {
    let commitment: serde_json::Value = serde_json::from_str(commitment_json)
        .map_err(|error| format!("decode stored vote commitment failed: {error}"))?;
    let shares = commitment
        .get("encrypted_shares")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "stored vote commitment has no encrypted share list".to_string())?;
    u32::try_from(shares.len())
        .map_err(|_| "required helper share count is outside the supported range".to_string())
}

pub fn vote_records(
    paths: &ProfilePaths,
    profile: Profile,
    round_id: &str,
) -> Result<Vec<VoteRecordView>, String> {
    if !paths.database.exists() {
        return Ok(Vec::new());
    }
    let db = open_db(paths, profile)?;
    let wallet = db.wallet_id();
    let conn = db.conn();
    let mut statement = conn
        .prepare(
            "SELECT bundle_index, proposal_id, choice, tx_hash, vc_tree_position
             FROM votes
             WHERE round_id = :round_id AND wallet_id = :wallet_id
             ORDER BY bundle_index, proposal_id",
        )
        .map_err(|error| format!("prepare vote recovery query failed: {error}"))?;
    let rows = statement
        .query_map(
            named_params! { ":round_id": round_id, ":wallet_id": wallet },
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                ))
            },
        )
        .map_err(|error| format!("query vote recovery state failed: {error}"))?;
    rows.map(|row| {
        let (bundle, proposal, choice, tx_hash, vc_position) =
            row.map_err(|error| format!("decode vote recovery row failed: {error}"))?;
        Ok(VoteRecordView {
            bundle_index: to_u32(bundle, "bundle index")?,
            proposal_id: to_u32(proposal, "proposal id")?,
            choice: to_u32(choice, "vote choice")?,
            tx_hash,
            vc_tree_position: vc_position
                .map(|value| to_u64(value, "vote commitment tree position"))
                .transpose()?,
        })
    })
    .collect()
}

pub fn export_backup(
    paths: &ProfilePaths,
    profile: Profile,
    passphrase: String,
) -> Result<BackupResult, String> {
    validate_passphrase(&passphrase)?;
    let passphrase = Zeroizing::new(passphrase);
    let manifest = read_manifest(paths, profile)?;

    let db = open_db(paths, profile)?;
    db.conn()
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .map_err(|error| format!("checkpoint voting database failed: {error}"))?;
    drop(db);
    let database = fs::read(&paths.database)
        .map_err(|error| format!("read voting database for backup failed: {error}"))?;

    let mut hotkeys = Vec::with_capacity(manifest.rounds.len());
    for round_id in manifest.rounds.keys() {
        let hotkey = load_hotkey(profile, round_id)?;
        hotkeys.push(BackupHotkey {
            round_id: round_id.clone(),
            stored_secret_base64: BASE64_STANDARD.encode(hotkey.stored_secret()),
        });
    }

    let envelope = BackupEnvelope {
        format_version: BACKUP_FORMAT_VERSION,
        profile,
        exported_at_unix_seconds: unix_seconds()?,
        manifest,
        voting_database_base64: BASE64_STANDARD.encode(database),
        hotkeys,
    };
    let plaintext = Zeroizing::new(
        serde_json::to_vec(&envelope)
            .map_err(|error| format!("encode backup envelope failed: {error}"))?,
    );
    let encrypted_bytes = encrypt_age(&passphrase, &plaintext)?;
    Ok(BackupResult {
        filename: format!(
            "valar-custody-voter-{}-{}.age",
            profile.slug(),
            envelope.exported_at_unix_seconds
        ),
        encrypted_bytes,
    })
}

pub fn restore_backup(
    paths: &ProfilePaths,
    profile: Profile,
    passphrase: String,
    encrypted_bytes: Vec<u8>,
) -> Result<RestoreResult, String> {
    validate_passphrase(&passphrase)?;
    if encrypted_bytes.is_empty() || encrypted_bytes.len() > MAX_BACKUP_BYTES {
        return Err(format!(
            "encrypted backup must contain 1..={MAX_BACKUP_BYTES} bytes"
        ));
    }
    let passphrase = Zeroizing::new(passphrase);
    let plaintext = Zeroizing::new(decrypt_age(&passphrase, &encrypted_bytes)?);
    let envelope: BackupEnvelope = serde_json::from_slice(&plaintext)
        .map_err(|error| format!("decode backup envelope failed: {error}"))?;
    validate_backup_envelope(&envelope, profile)?;

    let database = Zeroizing::new(
        BASE64_STANDARD
            .decode(envelope.voting_database_base64.as_bytes())
            .map_err(|error| format!("decode backed-up voting database failed: {error}"))?,
    );
    if database.is_empty() || database.len() > MAX_BACKUP_BYTES {
        return Err("backed-up voting database has an invalid size".to_string());
    }

    let temp_database = paths
        .directory
        .join(format!("voting.sqlite.restore-{}", Uuid::new_v4()));
    let _temp_database_cleanup = TemporaryDatabase(temp_database.clone());
    fs::write(&temp_database, &*database)
        .map_err(|error| format!("write restored database candidate failed: {error}"))?;
    {
        let db = VotingDb::open_path(&temp_database)
            .map_err(|error| format!("restored voting database is invalid: {error}"))?;
        db.set_wallet_id(&wallet_id(profile));
        let integrity: String = db
            .conn()
            .query_row("PRAGMA quick_check(1)", [], |row| row.get(0))
            .map_err(|error| format!("check restored voting database integrity failed: {error}"))?;
        if integrity != "ok" {
            return Err(format!(
                "restored voting database failed its integrity check: {integrity}"
            ));
        }
        let unexpected_wallet_rows: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM rounds WHERE wallet_id <> :wallet_id",
                named_params! { ":wallet_id": wallet_id(profile) },
                |row| row.get(0),
            )
            .map_err(|error| format!("inspect restored wallet identifiers failed: {error}"))?;
        if unexpected_wallet_rows != 0 {
            return Err(
                "restored voting database contains an unexpected wallet identifier".to_string(),
            );
        }
        for round in db
            .list_rounds()
            .map_err(|error| format!("inspect restored voting database failed: {error}"))?
        {
            if round.network != profile.network() {
                return Err(format!(
                    "restored round {} belongs to the wrong Zcash network",
                    round.round_id
                ));
            }
            let stored = envelope
                .manifest
                .rounds
                .get(&round.round_id)
                .ok_or_else(|| {
                    format!(
                        "restored voting database contains unexpected round {}",
                        round.round_id
                    )
                })?;
            let params = &stored.snapshot.params;
            let database_params: (u64, Vec<u8>, Vec<u8>, Vec<u8>) = db
                .conn()
                .query_row(
                    "SELECT snapshot_height, ea_pk, nc_root, nullifier_imt_root
                     FROM rounds
                     WHERE round_id = :round_id AND wallet_id = :wallet_id",
                    named_params! {
                        ":round_id": round.round_id,
                        ":wallet_id": wallet_id(profile),
                    },
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(|error| {
                    format!("inspect restored round {} failed: {error}", round.round_id)
                })?;
            if database_params
                != (
                    params.snapshot_height,
                    params.ea_pk.clone(),
                    params.nc_root.clone(),
                    params.nullifier_imt_root.clone(),
                )
            {
                return Err(format!(
                    "restored voting database parameters do not match manifest round {}",
                    round.round_id
                ));
            }
        }
        db.conn()
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(|error| format!("checkpoint restored voting database failed: {error}"))?;
    }

    let mut restored_hotkeys = BTreeMap::new();
    for hotkey in &envelope.hotkeys {
        let secret = Zeroizing::new(
            BASE64_STANDARD
                .decode(hotkey.stored_secret_base64.as_bytes())
                .map_err(|error| format!("decode backed-up hotkey failed: {error}"))?,
        );
        let reconstructed = VotingHotkey::from_stored_secret(&secret, profile.network())
            .map_err(|error| format!("backed-up hotkey is invalid: {error}"))?;
        let stored_round = envelope
            .manifest
            .rounds
            .get(&hotkey.round_id)
            .ok_or_else(|| format!("backed-up hotkey {} has no manifest round", hotkey.round_id))?;
        let target = crate::voter::target_json(
            &reconstructed,
            &stored_round.snapshot.chain_id,
            &stored_round.snapshot.params,
        )?;
        if target != stored_round.target_json {
            return Err(format!(
                "backed-up hotkey does not match target for round {}",
                hotkey.round_id
            ));
        }
        restored_hotkeys.insert(hotkey.round_id.clone(), secret);
    }

    let current_manifest = read_manifest(paths, profile)?;
    let affected_rounds = current_manifest
        .rounds
        .keys()
        .chain(envelope.manifest.rounds.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    let previous_hotkeys = capture_hotkeys(profile, &affected_rounds)?;
    let restore_result = (|| {
        for round_id in &affected_rounds {
            if let Some(secret) = restored_hotkeys.get(round_id) {
                store_hotkey(profile, round_id, secret)?;
            } else {
                delete_hotkey(profile, round_id)?;
            }
        }
        replace_restored_files(paths, &temp_database, &envelope.manifest)
    })();
    if let Err(error) = restore_result {
        let rollback = restore_captured_hotkeys(profile, &previous_hotkeys);
        return Err(match rollback {
            Ok(()) => error,
            Err(rollback_error) => {
                format!(
                    "{error}; restoring the previous Keychain state also failed: {rollback_error}"
                )
            }
        });
    }
    Ok(RestoreResult {
        profile,
        round_count: u32::try_from(envelope.manifest.rounds.len())
            .map_err(|_| "restored round count exceeds u32".to_string())?,
    })
}

fn validate_backup_envelope(envelope: &BackupEnvelope, profile: Profile) -> Result<(), String> {
    if envelope.format_version != BACKUP_FORMAT_VERSION {
        return Err(format!(
            "unsupported backup format version {}",
            envelope.format_version
        ));
    }
    if envelope.profile != profile {
        return Err(format!(
            "backup belongs to the {} profile, not {}",
            envelope.profile.slug(),
            profile.slug()
        ));
    }
    validate_manifest(&envelope.manifest, profile)?;
    let hotkey_rounds = envelope
        .hotkeys
        .iter()
        .map(|hotkey| hotkey.round_id.as_str())
        .collect::<BTreeSet<_>>();
    let manifest_rounds = envelope
        .manifest
        .rounds
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if hotkey_rounds.len() != envelope.hotkeys.len() || hotkey_rounds != manifest_rounds {
        return Err("backup hotkey set does not match its manifest rounds".to_string());
    }
    Ok(())
}

fn capture_hotkeys(
    profile: Profile,
    round_ids: &BTreeSet<String>,
) -> Result<CapturedHotkeys, String> {
    round_ids
        .iter()
        .map(|round_id| {
            let secret = match keyring_entry(profile, round_id)?.get_secret() {
                Ok(secret) => Some(Zeroizing::new(secret)),
                Err(keyring::Error::NoEntry) => None,
                Err(error) => {
                    return Err(format!(
                        "capture existing voting hotkey from operating-system keyring failed: {error}"
                    ));
                }
            };
            Ok((round_id.clone(), secret))
        })
        .collect()
}

fn restore_captured_hotkeys(profile: Profile, captured: &CapturedHotkeys) -> Result<(), String> {
    let mut errors = Vec::new();
    for (round_id, secret) in captured {
        let result = match secret {
            Some(secret) => store_hotkey(profile, round_id, secret),
            None => delete_hotkey(profile, round_id),
        };
        if let Err(error) = result {
            errors.push(format!("{round_id}: {error}"));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn reset_error_with_hotkey_rollback(
    error: String,
    profiles: &[(Profile, ProfilePaths, CapturedHotkeys)],
) -> String {
    let mut rollback_errors = Vec::new();
    for (profile, _, hotkeys) in profiles {
        if let Err(rollback_error) = restore_captured_hotkeys(*profile, hotkeys) {
            rollback_errors.push(rollback_error);
        }
    }
    if rollback_errors.is_empty() {
        error
    } else {
        format!(
            "{error}; restoring the previous Keychain state also failed: {}",
            rollback_errors.join("; ")
        )
    }
}

fn replace_restored_files(
    paths: &ProfilePaths,
    restored_database: &Path,
    manifest: &ProfileManifest,
) -> Result<(), String> {
    let previous_database = paths.directory.join("voting.sqlite.before-restore");
    let _ = fs::remove_file(&previous_database);
    let had_database = paths.database.exists();
    if had_database {
        fs::rename(&paths.database, &previous_database)
            .map_err(|error| format!("stage current voting database failed: {error}"))?;
    }
    if let Err(error) = fs::rename(restored_database, &paths.database) {
        if had_database {
            let _ = fs::rename(&previous_database, &paths.database);
        }
        return Err(format!("activate restored voting database failed: {error}"));
    }
    if let Err(error) = write_manifest(paths, manifest) {
        let _ = fs::remove_file(&paths.database);
        if had_database {
            let _ = fs::rename(&previous_database, &paths.database);
        }
        return Err(error);
    }
    let _ = fs::remove_file(previous_database);
    for suffix in ["-wal", "-shm"] {
        let stale = PathBuf::from(format!("{}{}", paths.database.display(), suffix));
        let _ = fs::remove_file(stale);
    }
    Ok(())
}

fn encrypt_age(passphrase: &str, plaintext: &[u8]) -> Result<Vec<u8>, String> {
    let secret = SecretString::from(passphrase.to_owned());
    let encryptor = age::Encryptor::with_user_passphrase(secret);
    let mut encrypted = Vec::new();
    let mut writer = encryptor
        .wrap_output(&mut encrypted)
        .map_err(|error| format!("start backup encryption failed: {error}"))?;
    writer
        .write_all(plaintext)
        .map_err(|error| format!("encrypt backup failed: {error}"))?;
    writer
        .finish()
        .map_err(|error| format!("finish backup encryption failed: {error}"))?;
    Ok(encrypted)
}

fn decrypt_age(passphrase: &str, encrypted: &[u8]) -> Result<Vec<u8>, String> {
    let decryptor = age::Decryptor::new(encrypted)
        .map_err(|error| format!("open encrypted backup failed: {error}"))?;
    let identity = age::scrypt::Identity::new(SecretString::from(passphrase.to_owned()));
    let mut reader = decryptor
        .decrypt(std::iter::once(&identity as &dyn age::Identity))
        .map_err(|error| format!("decrypt backup failed: {error}"))?;
    let mut plaintext = Vec::new();
    reader
        .read_to_end(&mut plaintext)
        .map_err(|error| format!("read decrypted backup failed: {error}"))?;
    Ok(plaintext)
}

fn validate_passphrase(passphrase: &str) -> Result<(), String> {
    let character_count = passphrase.chars().count();
    if character_count < MIN_BACKUP_PASSPHRASE_CHARS {
        return Err(format!(
            "backup passphrase must be at least {MIN_BACKUP_PASSPHRASE_CHARS} characters"
        ));
    }
    if character_count > MAX_BACKUP_PASSPHRASE_CHARS {
        return Err(format!(
            "backup passphrase must be at most {MAX_BACKUP_PASSPHRASE_CHARS} characters"
        ));
    }
    Ok(())
}

fn unix_seconds() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| format!("system clock is before Unix epoch: {error}"))
}

fn to_u32(value: i64, field: &str) -> Result<u32, String> {
    u32::try_from(value).map_err(|_| format!("{field} is outside the supported range"))
}

fn to_u64(value: i64, field: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("{field} is outside the supported range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn age_backup_encryption_round_trips_and_rejects_wrong_password() {
        let plaintext = b"voting secrets stay encrypted";
        let encrypted = encrypt_age("correct horse battery staple", plaintext).unwrap();
        assert_ne!(encrypted, plaintext);
        assert_eq!(
            decrypt_age("correct horse battery staple", &encrypted).unwrap(),
            plaintext
        );
        assert!(decrypt_age("wrong password", &encrypted).is_err());
    }

    #[test]
    fn backup_validation_rejects_duplicate_hotkey_rounds() {
        let first = crate::demo::round_snapshot();
        let mut second = first.clone();
        second.round_id = "11".repeat(32);
        second.params.vote_round_id = second.round_id.clone();
        let mut manifest = ProfileManifest::empty(Profile::Demo);
        for round in [first.clone(), second] {
            manifest.rounds.insert(
                round.round_id.clone(),
                crate::model::StoredRound {
                    snapshot: round,
                    target_json: "target".to_string(),
                    capability_digest: None,
                },
            );
        }
        let envelope = BackupEnvelope {
            format_version: BACKUP_FORMAT_VERSION,
            profile: Profile::Demo,
            exported_at_unix_seconds: 0,
            manifest,
            voting_database_base64: String::new(),
            hotkeys: vec![
                BackupHotkey {
                    round_id: first.round_id.clone(),
                    stored_secret_base64: "secret-a".to_string(),
                },
                BackupHotkey {
                    round_id: first.round_id,
                    stored_secret_base64: "secret-b".to_string(),
                },
            ],
        };
        assert!(validate_backup_envelope(&envelope, Profile::Demo).is_err());
    }

    #[test]
    fn round_progress_distinguishes_confirmed_votes_from_complete_share_delivery() {
        let database =
            std::env::temp_dir().join(format!("custody-voter-progress-{}.sqlite", Uuid::new_v4()));
        let _database = TemporaryDatabase(database.clone());
        let paths = ProfilePaths {
            directory: std::env::temp_dir(),
            manifest: database.with_extension("json"),
            database,
        };
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
        for (proposal_id, commitment_json) in [
            (1, r#"{"encrypted_shares":[{},{}]}"#),
            (2, r#"{"encrypted_shares":[{},{},{}]}"#),
        ] {
            db.conn()
                .execute(
                    "INSERT INTO votes (
                        round_id, wallet_id, bundle_index, proposal_id, choice, created_at,
                        tx_hash, vc_tree_position, commitment_bundle_json
                     ) VALUES ('round', ?1, 0, ?2, 0, 1, 'tx', 8, ?3)",
                    rusqlite::params![wallet, proposal_id, commitment_json],
                )
                .unwrap();
        }
        for (proposal_id, share_index) in [(1, 0), (1, 1), (2, 0), (2, 1)] {
            db.conn()
                .execute(
                    "INSERT INTO share_delegations (
                        round_id, wallet_id, bundle_index, proposal_id, share_index,
                        sent_to_urls, nullifier, confirmed, submit_at, created_at
                     ) VALUES ('round', ?1, 0, ?2, ?3, '[]', X'00', 1, 1, 1)",
                    rusqlite::params![wallet, proposal_id, share_index],
                )
                .unwrap();
        }
        drop(db);

        let progress = round_progress(&paths, Profile::Testnet, "round", true).unwrap();
        assert_eq!(progress.vote_count, 2);
        assert_eq!(progress.confirmed_vote_count, 2);
        assert_eq!(progress.required_share_count, 5);
        assert_eq!(progress.submitted_share_count, 4);
    }

    #[test]
    fn reset_all_data_removes_every_existing_profile_directory() {
        let root = std::env::temp_dir().join(format!("custody-voter-reset-{}", Uuid::new_v4()));
        fs::create_dir_all(&root).unwrap();
        for profile in [Profile::Mainnet, Profile::Testnet] {
            let paths = profile_paths(&root, profile).unwrap();
            write_manifest(&paths, &ProfileManifest::empty(profile)).unwrap();
            fs::write(paths.directory.join("profile-state"), b"state").unwrap();
        }
        let test_custodian = root.join("test-custodian").join("testnet");
        fs::create_dir_all(&test_custodian).unwrap();
        fs::write(test_custodian.join("provider-voting.sqlite"), b"state").unwrap();

        let result = reset_all_data(&root).unwrap();
        assert_eq!(result.removed_profiles, 2);
        assert_eq!(result.removed_rounds, 0);
        assert!(!root.exists());
    }
}
