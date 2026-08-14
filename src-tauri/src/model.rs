use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use zcash_voting::{Network, VotingRoundParams};
use zeroize::Zeroize;

pub const MANIFEST_FORMAT_VERSION: u32 = 1;
pub const BACKUP_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    Mainnet,
    Testnet,
    Demo,
}

impl Profile {
    pub fn slug(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
            Self::Demo => "demo",
        }
    }

    pub fn network(self) -> Network {
        match self {
            Self::Mainnet => Network::Mainnet,
            Self::Testnet => Network::Testnet,
            Self::Demo => Network::Regtest,
        }
    }

    pub fn network_name(self) -> &'static str {
        match self {
            Self::Mainnet => "mainnet",
            Self::Testnet => "testnet",
            Self::Demo => "regtest",
        }
    }

    pub fn chain_id(self) -> &'static str {
        match self {
            Self::Mainnet => "zvote-1",
            Self::Testnet => "svote-1",
            Self::Demo => "custody-voter-demo-1",
        }
    }

    pub fn is_demo(self) -> bool {
        self == Self::Demo
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AppOption {
    pub index: u32,
    pub label: String,
    pub description: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AppProposal {
    pub id: u32,
    pub title: String,
    pub description: String,
    pub options: Vec<AppOption>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceEndpointView {
    pub url: String,
    pub label: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RoundSnapshot {
    pub profile: Profile,
    pub chain_id: String,
    pub round_id: String,
    pub title: String,
    pub description: String,
    pub status: String,
    pub status_label: String,
    pub is_active: bool,
    pub snapshot_height: u64,
    pub vote_end_time: u64,
    pub ceremony_start_time: Option<u64>,
    pub proposals: Vec<AppProposal>,
    pub params: VotingRoundParams,
    pub vote_servers: Vec<ServiceEndpointView>,
    pub authenticated: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RoundProgress {
    pub target_ready: bool,
    pub capability_imported: bool,
    pub bundle_count: u32,
    pub confirmed_bundle_count: u32,
    pub delegated_ballots: u64,
    pub vote_count: u32,
    pub submitted_vote_count: u32,
    pub confirmed_vote_count: u32,
    pub required_share_count: u32,
    pub submitted_share_count: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RoundCard {
    #[serde(flatten)]
    pub round: RoundSnapshot,
    pub progress: RoundProgress,
    pub stored: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VoteRecordView {
    pub bundle_index: u32,
    pub proposal_id: u32,
    pub choice: u32,
    pub tx_hash: Option<String>,
    pub vc_tree_position: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RoundWorkspace {
    pub round: RoundSnapshot,
    pub progress: RoundProgress,
    pub target_json: Option<String>,
    pub capability_digest: Option<String>,
    pub votes: Vec<VoteRecordView>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TargetResult {
    pub target_json: String,
    pub reused: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ImportResult {
    pub digest: String,
    pub bundle_count: u32,
    pub confirmed_bundle_count: u32,
    pub pending_transaction_hashes: Vec<String>,
}

#[cfg(debug_assertions)]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TestCustodianResult {
    pub capability_json: String,
    pub digest: String,
    pub bundle_count: u32,
    pub submitted_bundle_count: u32,
    pub delegation_transaction_hashes: Vec<String>,
}

#[cfg(debug_assertions)]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestCustodianProgressEvent {
    pub round_id: String,
    pub phase: String,
    pub bundle_index: Option<u32>,
    pub bundle_count: Option<u32>,
    pub progress: Option<f64>,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VoteChoiceInput {
    pub proposal_id: u32,
    pub choice: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct VoteTransactionResult {
    pub bundle_index: u32,
    pub proposal_id: u32,
    pub tx_hash: String,
    pub vc_tree_position: u64,
    pub shares_submitted: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CastVotesResult {
    pub demo: bool,
    pub proof_count: u32,
    pub transactions: Vec<VoteTransactionResult>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VoteProgressEvent {
    pub round_id: String,
    pub phase: String,
    pub bundle_index: Option<u32>,
    pub proposal_id: Option<u32>,
    pub progress: Option<f64>,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StoredRound {
    pub snapshot: RoundSnapshot,
    pub target_json: String,
    pub capability_digest: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProfileManifest {
    pub format_version: u32,
    pub profile: Profile,
    pub rounds: BTreeMap<String, StoredRound>,
}

impl ProfileManifest {
    pub fn empty(profile: Profile) -> Self {
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            profile,
            rounds: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackupHotkey {
    pub round_id: String,
    pub stored_secret_base64: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BackupEnvelope {
    pub format_version: u32,
    pub profile: Profile,
    pub exported_at_unix_seconds: u64,
    pub manifest: ProfileManifest,
    pub voting_database_base64: String,
    pub hotkeys: Vec<BackupHotkey>,
}

impl Drop for BackupEnvelope {
    fn drop(&mut self) {
        self.voting_database_base64.zeroize();
        for hotkey in &mut self.hotkeys {
            hotkey.stored_secret_base64.zeroize();
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BackupResult {
    pub filename: String,
    pub encrypted_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RestoreResult {
    pub profile: Profile,
    pub round_count: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResetResult {
    pub removed_profiles: u32,
    pub removed_rounds: u32,
}
