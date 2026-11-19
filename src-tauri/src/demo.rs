use std::{collections::HashMap, sync::Arc};

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use rusqlite::{named_params, OptionalExtension};
use sha2::{Digest, Sha256};
use tauri::AppHandle;
use vote_commitment_tree::MemoryTreeServer;
use zcash_voting::{
    backend::pasta_curves::{
        group::{ff::PrimeField, Group, GroupEncoding},
        pallas, Fp,
    },
    delegation_capability::{DelegationCapabilityBundleV1, DelegationCapabilityV1},
    round::VotingDb,
    session::Decision,
    vote::{commit_batch, recover_signed_commitments, DraftVote, VanWitness, VoteSigner},
    AdvanceVote, ChainHttpRequest, ChainHttpResponse, ChainSubmissionClient,
    ChainSubmissionControl, ChainSubmissionResult, ChainTransport, ChainTransportFuture,
    VoteCommitStageBridge, VotingHotkey,
};
use zeroize::Zeroizing;

use crate::{
    model::{AppOption, AppProposal, Profile, RoundSnapshot, VoteTransactionResult},
    storage::RECORDED_VOTE,
};

/// Base URL of the rehearsal's in-process vote chain. Nothing listens on it.
pub const CHAIN_URL: &str = "http://custody-voter-demo.invalid";

/// Lifecycle passes before the rehearsal stops waiting on a confirmation that
/// the in-process chain answers on the first lookup.
const MAX_CONFIRMATION_PASSES: usize = 4;

pub fn round_snapshot() -> RoundSnapshot {
    let round_id = hex::encode(Fp::from(4_242u64).to_repr());
    RoundSnapshot {
        profile: Profile::Demo,
        chain_id: Profile::Demo.chain_id().to_string(),
        round_id: round_id.clone(),
        title: "Local custody handoff demo".to_string(),
        description: "A private, offline rehearsal using real capability validation and vote proofs. Nothing is broadcast.".to_string(),
        status: "active".to_string(),
        status_label: "Local demo".to_string(),
        is_active: true,
        snapshot_height: 100,
        vote_end_time: 4_102_444_800,
        ceremony_start_time: Some(1_700_000_000),
        proposals: vec![
            AppProposal {
                id: 1,
                title: "Choose the launch snack".to_string(),
                description: "This proposal exists only to exercise the complete vote path.".to_string(),
                options: vec![
                    AppOption { index: 0, label: "Tacos".to_string(), description: "Portable and configurable.".to_string() },
                    AppOption { index: 1, label: "Dumplings".to_string(), description: "Compact and excellent.".to_string() },
                    AppOption { index: 2, label: "Waffles".to_string(), description: "Maximum surface area.".to_string() },
                ],
            },
            AppProposal {
                id: 2,
                title: "Is the custody voter ready?".to_string(),
                description: "A second proposal verifies multi-question recovery state.".to_string(),
                options: vec![
                    AppOption { index: 0, label: "Yes".to_string(), description: String::new() },
                    AppOption { index: 1, label: "Not yet".to_string(), description: String::new() },
                ],
            },
        ],
        params: zcash_voting::VotingRoundParams {
            vote_round_id: round_id,
            snapshot_height: 100,
            ea_pk: pallas::Point::generator().to_bytes().to_vec(),
            nc_root: Fp::from(7u64).to_repr().to_vec(),
            nullifier_imt_root: Fp::from(8u64).to_repr().to_vec(),
        },
        vote_servers: Vec::new(),
        authenticated: true,
    }
}

pub fn capability_json(hotkey: &VotingHotkey) -> Result<String, String> {
    let round = round_snapshot();
    let target = hotkey.delegation_target();
    let bundles = [12u64, 7u64]
        .into_iter()
        .enumerate()
        .map(|(index, num_ballots)| DelegationCapabilityBundleV1 {
            bundle_index: index as u32,
            num_ballots,
            van_comm_rand: BASE64_STANDARD.encode(Fp::from(100 + index as u64).to_repr()),
            delegation_tx_hash: hex::encode(Sha256::digest(format!(
                "custody-voter-demo-delegation-{}-{index}",
                round.round_id
            ))),
        })
        .collect();
    let capability = DelegationCapabilityV1 {
        format_version: 1,
        vote_chain_id: round.chain_id,
        network: Profile::Demo.network_name().to_string(),
        vote_round_id: round.round_id,
        address_index: target.address_index(),
        raw_orchard_address: BASE64_STANDARD.encode(target.raw_orchard_address()),
        bundles,
    };
    String::from_utf8(
        capability
            .to_json()
            .map_err(|error| format!("build demo capability failed: {error}"))?,
    )
    .map_err(|error| format!("demo capability is not UTF-8: {error}"))
}

/// Confirms the imported demo delegations through the SDK's own lifecycle.
///
/// The in-process chain reports bundle `n`'s VAN at leaf `n`, the order the
/// rehearsal tree appends them in.
pub async fn confirm_delegations(db: &Arc<VotingDb>, round: &RoundSnapshot) -> Result<(), String> {
    let hashes = delegation_hashes(db, &round.round_id)?;
    if hashes.is_empty() {
        return Err("import the demo custody payload before confirming it".to_string());
    }
    let chain = Arc::new(DemoChain {
        round_id: round.round_id.clone(),
        transactions: DemoTransactions::Delegations(hashes),
    });
    for _ in 0..MAX_CONFIRMATION_PASSES {
        crate::voter::advance_imported_delegations(db, Arc::clone(&chain), Profile::Demo, round)
            .await?;
        let plan = crate::voter::round_plan(db, round)?;
        if plan.delegation_bundles_needing_work.is_empty() {
            return Ok(());
        }
    }
    Err("demo delegations did not confirm on the local chain".to_string())
}

/// Proves every chosen vote locally and confirms it on the in-process chain.
///
/// Votes go one at a time through the singleton cast-vote route, so each
/// witness comes from a local tree holding every earlier confirmed vote. The
/// live path batches a bundle's votes instead, which needs the vote-tree sync
/// protocol this rehearsal does not serve. There is no helper fleet here, so
/// no share is delivered.
pub async fn cast_votes(
    app: &AppHandle,
    db: &Arc<VotingDb>,
    round: &RoundSnapshot,
    hotkey: &VotingHotkey,
) -> Result<Vec<VoteTransactionResult>, String> {
    let choices = db
        .ballot_intents(&round.round_id)
        .map_err(|error| format!("load demo ballot failed: {error}"))?
        .into_iter()
        .filter_map(|(proposal_id, decision)| match decision {
            Decision::Choice(choice) => Some((proposal_id, choice)),
            Decision::Skipped => None,
        })
        .collect::<Vec<_>>();
    let bundle_count = db
        .get_bundle_count(&round.round_id)
        .map_err(|error| format!("load demo bundle count failed: {error}"))?;
    let secret = Zeroizing::new(hotkey.stored_secret().to_vec());
    let mut transactions = Vec::new();
    for bundle_index in 0..bundle_count {
        for &(proposal_id, choice) in &choices {
            if confirmed_vote(db, &round.round_id, bundle_index, proposal_id)?.is_none() {
                if !vote_exists(db, &round.round_id, bundle_index, proposal_id)? {
                    commit_vote(app, db, round, bundle_index, proposal_id, choice, &secret).await?;
                }
                confirm_vote(db, round, bundle_index, proposal_id).await?;
            }
            let (tx_hash, vc_tree_position) =
                confirmed_vote(db, &round.round_id, bundle_index, proposal_id)?
                    .ok_or_else(|| "demo vote did not confirm on the local chain".to_string())?;
            transactions.push(VoteTransactionResult {
                bundle_index,
                proposal_id,
                tx_hash,
                vc_tree_position,
                shares_submitted: 0,
            });
        }
    }
    Ok(transactions)
}

async fn commit_vote(
    app: &AppHandle,
    db: &Arc<VotingDb>,
    round: &RoundSnapshot,
    bundle_index: u32,
    proposal_id: u32,
    choice: u32,
    secret: &Zeroizing<Vec<u8>>,
) -> Result<(), String> {
    let num_options = round
        .proposals
        .iter()
        .find(|proposal| proposal.id == proposal_id)
        .map(|proposal| proposal.options.len() as u32)
        .ok_or_else(|| format!("demo proposal {proposal_id} is unknown"))?;
    let db = Arc::clone(db);
    let round_id = round.round_id.clone();
    let secret = secret.clone();
    let progress_app = app.clone();
    tauri::async_runtime::spawn_blocking(move || {
        let witness = witness(&db, &round_id, bundle_index)?;
        let hotkey = VotingHotkey::from_stored_secret(&secret, Profile::Demo.network())
            .map_err(|error| format!("reconstruct voting hotkey failed: {error}"))?;
        let progress_round = round_id.clone();
        let reporter = VoteCommitStageBridge::new(move |stage| {
            crate::voter::report_commit_stage(&progress_app, &progress_round, stage);
        });
        commit_batch(
            &db,
            &round_id,
            bundle_index,
            &[DraftVote {
                proposal_id,
                choice,
                num_options,
                vc_tree_position: 0,
                single_share: true,
            }],
            &witness,
            VoteSigner::hotkey(&hotkey),
            &reporter,
        )
        .map(|_| ())
        .map_err(|error| format!("build signed vote commitment failed: {error}"))
    })
    .await
    .map_err(|error| format!("demo vote proof task failed: {error}"))?
}

/// Drives one committed demo vote to confirmation through the SDK lifecycle.
///
/// The vote takes the next two leaves after every confirmed one, which is
/// where a chain would append its new VAN and vote commitment.
async fn confirm_vote(
    db: &Arc<VotingDb>,
    round: &RoundSnapshot,
    bundle_index: u32,
    proposal_id: u32,
) -> Result<(), String> {
    let (tree, _) = rebuild_tree(db, &round.round_id)?;
    let chain = DemoChain {
        round_id: round.round_id.clone(),
        transactions: DemoTransactions::Vote {
            van_position: tree.size(),
        },
    };
    let client = ChainSubmissionClient::with_transport(
        Arc::clone(db),
        chain,
        zcash_voting::ChainSubmissionClientConfig::for_network(
            Profile::Demo.network(),
            vec![CHAIN_URL.to_string()],
        )
        .with_vote_chain_id(round.chain_id.clone()),
    )
    .map_err(|failure| format!("configure demo vote chain failed: {}", failure.message()))?;
    let vote_round_id = hex::decode(&round.round_id)
        .ok()
        .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
        .ok_or_else(|| "demo round id must be 32 bytes of hex".to_string())?;
    let control = ChainSubmissionControl::new(0);
    for _ in 0..MAX_CONFIRMATION_PASSES {
        let result = client
            .advance_vote(
                AdvanceVote {
                    vote_round_id,
                    bundle_index,
                    proposal_id,
                },
                &control,
            )
            .await
            .map_err(|failure| format!("confirm demo vote failed: {}", failure.message()))?;
        match result {
            ChainSubmissionResult::Confirmed(_) => return Ok(()),
            ChainSubmissionResult::Pending(_) => {}
            ChainSubmissionResult::Rejected(diagnostic)
            | ChainSubmissionResult::SubmittedWithoutHash(diagnostic) => {
                return Err(format!(
                    "the local demo chain refused a vote: {}",
                    diagnostic.message()
                ))
            }
            _ => return Err("confirming the demo vote was cancelled".to_string()),
        }
    }
    Err("demo vote did not confirm on the local chain".to_string())
}

/// What the in-process chain has committed for one lifecycle call.
enum DemoTransactions {
    /// Imported delegation hashes, each with its bundle's VAN leaf.
    Delegations(HashMap<String, u32>),
    /// The one vote being confirmed. Any lookup is for it, because the
    /// rehearsal confirms votes one at a time.
    Vote { van_position: u64 },
}

/// In-process stand-in for the vote chain's transaction API.
///
/// It serves only what the SDK lifecycle asks of an imported delegation and a
/// singleton vote: transaction lookups and the cast-vote POST.
struct DemoChain {
    round_id: String,
    transactions: DemoTransactions,
}

impl DemoChain {
    fn lookup(&self, url: &str) -> ChainHttpResponse {
        let Some(hash) = url.strip_prefix(&format!("{CHAIN_URL}/shielded-vote/v1/tx/")) else {
            return not_found();
        };
        let (event_type, leaf_index) = match &self.transactions {
            DemoTransactions::Delegations(hashes) => match hashes.get(hash) {
                Some(bundle_index) => ("delegate_vote", bundle_index.to_string()),
                None => return not_found(),
            },
            DemoTransactions::Vote { van_position } => {
                ("cast_vote", format!("{van_position},{}", van_position + 1))
            }
        };
        let body = serde_json::json!({
            "height": "1",
            "code": 0,
            "log": "",
            "events": [{
                "type": event_type,
                "attributes": [
                    { "key": "vote_round_id", "value": self.round_id },
                    { "key": "leaf_index", "value": leaf_index },
                ],
            }],
        });
        ChainHttpResponse::json(200, body.to_string().into_bytes())
    }
}

impl ChainTransport for DemoChain {
    fn chain_get<'a>(&'a self, request: ChainHttpRequest) -> ChainTransportFuture<'a> {
        let response = self.lookup(request.url());
        Box::pin(async move { Ok(response) })
    }

    fn chain_post_json<'a>(
        &'a self,
        request: ChainHttpRequest,
        json: Vec<u8>,
    ) -> ChainTransportFuture<'a> {
        let accepts = matches!(self.transactions, DemoTransactions::Vote { .. })
            && request.url() == format!("{CHAIN_URL}/shielded-vote/v1/cast-vote");
        let response = if accepts {
            let body = serde_json::json!({
                "tx_hash": hex::encode(Sha256::digest(&json)),
                "code": 0,
            });
            ChainHttpResponse::json(200, body.to_string().into_bytes())
        } else {
            not_found()
        };
        Box::pin(async move { Ok(response) })
    }
}

fn not_found() -> ChainHttpResponse {
    ChainHttpResponse::json(404, br#"{"error":"tx not found"}"#.to_vec())
}

fn delegation_hashes(db: &VotingDb, round_id: &str) -> Result<HashMap<String, u32>, String> {
    let wallet = db.wallet_id();
    let conn = db.conn();
    let mut statement = conn
        .prepare(
            "SELECT bundle_index, delegation_tx_hash
             FROM bundles
             WHERE round_id = :round_id AND wallet_id = :wallet_id
             ORDER BY bundle_index",
        )
        .map_err(|error| format!("prepare demo delegation query failed: {error}"))?;
    let rows = statement
        .query_map(
            named_params! { ":round_id": round_id, ":wallet_id": wallet },
            |row| Ok((row.get::<_, String>(1)?, row.get::<_, u32>(0)?)),
        )
        .map_err(|error| format!("query demo delegations failed: {error}"))?;
    rows.collect::<Result<HashMap<_, _>, _>>()
        .map_err(|error| format!("decode demo delegation failed: {error}"))
}

/// Whether a signed vote is waiting to be confirmed, so it resumes instead of
/// being built again.
fn vote_exists(
    db: &VotingDb,
    round_id: &str,
    bundle_index: u32,
    proposal_id: u32,
) -> Result<bool, String> {
    let wallet = db.wallet_id();
    db.conn()
        .query_row(
            &format!(
                "SELECT EXISTS(
                    SELECT 1 FROM votes
                    WHERE round_id = :round_id AND wallet_id = :wallet_id
                      AND bundle_index = :bundle_index AND proposal_id = :proposal_id
                      AND {RECORDED_VOTE})"
            ),
            named_params! {
                ":round_id": round_id,
                ":wallet_id": wallet,
                ":bundle_index": bundle_index,
                ":proposal_id": proposal_id,
            },
            |row| row.get(0),
        )
        .map_err(|error| format!("load demo vote failed: {error}"))
}

fn confirmed_vote(
    db: &VotingDb,
    round_id: &str,
    bundle_index: u32,
    proposal_id: u32,
) -> Result<Option<(String, u64)>, String> {
    let wallet = db.wallet_id();
    db.conn()
        .query_row(
            "SELECT tx_hash, vc_tree_position FROM votes
             WHERE round_id = :round_id AND wallet_id = :wallet_id
               AND bundle_index = :bundle_index AND proposal_id = :proposal_id
               AND tx_hash IS NOT NULL AND vc_tree_position IS NOT NULL",
            named_params! {
                ":round_id": round_id,
                ":wallet_id": wallet,
                ":bundle_index": bundle_index,
                ":proposal_id": proposal_id,
            },
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| format!("load demo vote confirmation failed: {error}"))
}

pub fn witness(db: &VotingDb, round_id: &str, bundle_index: u32) -> Result<VanWitness, String> {
    let (tree, current_positions) = rebuild_tree(db, round_id)?;
    let position = current_positions
        .iter()
        .find(|(index, _)| *index == bundle_index)
        .map(|(_, position)| *position)
        .ok_or_else(|| format!("demo bundle {bundle_index} has no confirmed VAN position"))?;
    let anchor_height = 1;
    let path = tree
        .path(u64::from(position), anchor_height)
        .ok_or_else(|| format!("build demo witness for position {position} failed"))?;
    Ok(VanWitness::from((path, anchor_height)))
}

fn rebuild_tree(
    db: &VotingDb,
    round_id: &str,
) -> Result<(MemoryTreeServer, Vec<(u32, u32)>), String> {
    let wallet = db.wallet_id();
    let (initial_vans, mut current_positions, vote_keys) = {
        let conn = db.conn();
        let mut bundle_statement = conn
            .prepare(
                "SELECT bundle_index, gov_comm, van_leaf_position
                 FROM bundles
                 WHERE round_id = :round_id AND wallet_id = :wallet_id
                 ORDER BY bundle_index",
            )
            .map_err(|error| format!("prepare demo tree bundle query failed: {error}"))?;
        let bundles = bundle_statement
            .query_map(
                named_params! { ":round_id": round_id, ":wallet_id": wallet },
                |row| {
                    Ok((
                        row.get::<_, u32>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, Option<u32>>(2)?,
                    ))
                },
            )
            .map_err(|error| format!("query demo tree bundles failed: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("decode demo tree bundle failed: {error}"))?;

        let mut vote_statement = conn
            .prepare(
                "SELECT bundle_index, proposal_id, vc_tree_position
                 FROM votes
                 WHERE round_id = :round_id AND wallet_id = :wallet_id
                   AND vc_tree_position IS NOT NULL
                 ORDER BY vc_tree_position",
            )
            .map_err(|error| format!("prepare demo tree vote query failed: {error}"))?;
        let votes = vote_statement
            .query_map(
                named_params! { ":round_id": round_id, ":wallet_id": wallet },
                |row| {
                    Ok((
                        row.get::<_, u32>(0)?,
                        row.get::<_, u32>(1)?,
                        row.get::<_, u64>(2)?,
                    ))
                },
            )
            .map_err(|error| format!("query demo tree votes failed: {error}"))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("decode demo tree vote failed: {error}"))?;

        let initial_vans = bundles
            .iter()
            .map(|(index, bytes, _)| Ok((*index, decode_field(bytes, "demo VAN commitment")?)))
            .collect::<Result<Vec<_>, String>>()?;
        let positions = bundles
            .iter()
            .map(|(index, _, position)| {
                position
                    .map(|position| (*index, position))
                    .ok_or_else(|| format!("demo bundle {index} is not confirmed"))
            })
            .collect::<Result<Vec<_>, String>>()?;
        (initial_vans, positions, votes)
    };

    let mut tree = MemoryTreeServer::empty();
    for (expected, (bundle_index, van)) in initial_vans.into_iter().enumerate() {
        if bundle_index != expected as u32 {
            return Err("demo bundle indices are not contiguous".to_string());
        }
        tree.append(van)
            .map_err(|error| format!("append demo delegation VAN failed: {error:?}"))?;
    }
    for (bundle_index, proposal_id, expected_vc_position) in vote_keys {
        let signed = recover_signed_commitments(db, round_id, bundle_index, proposal_id)
            .map_err(|error| format!("recover demo vote commitment failed: {error}"))?;
        let commitment = signed
            .commitments
            .first()
            .ok_or_else(|| "recovered demo vote has no commitment".to_string())?;
        let expected_van_position = expected_vc_position
            .checked_sub(1)
            .ok_or_else(|| "demo vote has invalid VC tree position".to_string())?;
        if tree.size() != expected_van_position {
            return Err("demo vote tree positions are not contiguous".to_string());
        }
        tree.append_two(
            decode_field(&commitment.vote_authority_note_new, "new demo VAN")?,
            decode_field(&commitment.vote_commitment, "demo vote commitment")?,
        )
        .map_err(|error| format!("append demo vote leaves failed: {error:?}"))?;
        let current_position = current_positions
            .iter_mut()
            .find(|(index, _)| *index == bundle_index)
            .ok_or_else(|| format!("demo vote references unknown bundle {bundle_index}"))?;
        current_position.1 = u32::try_from(expected_van_position)
            .map_err(|_| "demo VAN position exceeds u32".to_string())?;
    }
    tree.checkpoint(1)
        .map_err(|error| format!("checkpoint demo vote tree failed: {error:?}"))?;
    Ok((tree, current_positions))
}

fn decode_field(bytes: &[u8], field: &str) -> Result<Fp, String> {
    let repr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| format!("{field} must contain 32 bytes"))?;
    Option::<Fp>::from(Fp::from_repr(repr))
        .ok_or_else(|| format!("{field} is not a canonical Pallas field element"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use zcash_voting::{
        delegation_capability::{import_delegation_capability, ImportDelegationCapabilityParams},
        hotkey::generate_random_voting_hotkey,
        NoopProgressReporter,
    };

    fn imported_demo_round() -> (Arc<VotingDb>, RoundSnapshot, VotingHotkey) {
        let round = round_snapshot();
        let hotkey = generate_random_voting_hotkey(Profile::Demo.network()).unwrap();
        let capability = capability_json(&hotkey).unwrap();
        let db = Arc::new(VotingDb::open_in_memory().unwrap());
        db.set_wallet_id("demo-test");
        let digest = import_delegation_capability(
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
        assert_eq!(digest.to_hex().len(), 64);
        (db, round, hotkey)
    }

    #[tokio::test]
    async fn demo_capability_confirms_through_the_sdk_lifecycle() {
        let (db, round, _) = imported_demo_round();
        assert_eq!(db.get_bundle_count(&round.round_id).unwrap(), 2);

        confirm_delegations(&db, &round).await.unwrap();

        let plan = crate::voter::round_plan(&db, &round).unwrap();
        assert!(plan
            .delegation_statuses
            .iter()
            .all(|status| status.phase == zcash_voting::phases::DelegationPhase::Confirmed));
        let witness = witness(&db, &round.round_id, 1).unwrap();
        assert_eq!(witness.position, 1);
        assert_eq!(witness.auth_path.len(), 24);
    }

    #[tokio::test]
    #[ignore = "generates full zero-knowledge vote proofs"]
    async fn demo_generates_and_confirms_sequential_vote_proofs() {
        let (db, round, hotkey) = imported_demo_round();
        confirm_delegations(&db, &round).await.unwrap();
        db.set_ballot_intents(
            &round.round_id,
            Profile::Demo.network(),
            &[(1, Decision::Choice(1), 3), (2, Decision::Choice(0), 2)],
        )
        .unwrap();

        for proposal_id in [1, 2] {
            let initial_witness = witness(&db, &round.round_id, 0).unwrap();
            commit_batch(
                &db,
                &round.round_id,
                0,
                &[DraftVote {
                    proposal_id,
                    choice: if proposal_id == 1 { 1 } else { 0 },
                    num_options: if proposal_id == 1 { 3 } else { 2 },
                    vc_tree_position: 0,
                    single_share: true,
                }],
                &initial_witness,
                VoteSigner::hotkey(&hotkey),
                &NoopProgressReporter,
            )
            .unwrap();
            confirm_vote(&db, &round, 0, proposal_id).await.unwrap();
        }

        let (tx_hash, vc_position) = confirmed_vote(&db, &round.round_id, 0, 1).unwrap().unwrap();
        assert_eq!(tx_hash.len(), 64);
        assert_eq!(vc_position, 3);
        assert_eq!(
            confirmed_vote(&db, &round.round_id, 0, 2)
                .unwrap()
                .unwrap()
                .1,
            5
        );
        assert_eq!(witness(&db, &round.round_id, 0).unwrap().position, 4);
    }
}
