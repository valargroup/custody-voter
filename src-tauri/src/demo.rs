//! Offline chain and helper transports. Every state transition still passes
//! through the same SDK lifecycle as a live vote; these transports never use a socket.
use crate::{
    model::{AppOption, AppProposal, Profile, RoundSnapshot, ServiceEndpointView},
    voter,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use ff::PrimeField;
use group::{Group, GroupEncoding};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use vote_commitment_tree::{sync_api::TreeSyncApi, MemoryTreeServer};
use vote_commitment_tree_client::transport::{Transport, TransportError, TransportResponse};
use zcash_voting::backend::pasta_curves::{pallas, Fp};
use zcash_voting::{
    delegation_capability::{DelegationCapabilityBundleV1, DelegationCapabilityV1},
    helper::transport::{HelperFuture, HelperResponse, HelperTransport},
    prelude::*,
    vote::{recovery_bundle, VoteRecoveryBundle},
};

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
        vote_servers: vec![ServiceEndpointView {url:"http://custody-demo.invalid".into(),label:"Local simulation".into()}],
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

pub struct DemoTransport {
    db: Arc<VotingDb>,
    round_id: String,
    state: Mutex<DemoChain>,
}
struct DemoChain {
    tree: MemoryTreeServer,
    transactions: HashMap<String, Value>,
    bodies: HashMap<Vec<u8>, Value>,
    height: u32,
}

fn event(kind: &str, round: &str, mut attributes: Vec<(&str, String)>) -> Value {
    attributes.push(("vote_round_id", round.into()));
    json!({"type":kind,"attributes":attributes.into_iter().map(|(key,value)|json!({"key":key,"value":value})).collect::<Vec<_>>()})
}
fn confirmed(height: u32, event: Value) -> Value {
    json!({"height":height.to_string(),"code":0,"log":"","events":[event]})
}
fn field(bytes: &[u8]) -> Result<Fp, String> {
    let repr: [u8; 32] = bytes.try_into().map_err(|_| "invalid demo field length")?;
    Option::from(Fp::from_repr(repr)).ok_or_else(|| "invalid demo field".into())
}
fn append(tree: &mut MemoryTreeServer, bytes: &[u8]) -> Result<(), String> {
    tree.append(field(bytes)?)
        .map_err(|e| format!("append demo leaf: {e:?}"))?;
    Ok(())
}

impl DemoTransport {
    pub fn new(db: Arc<VotingDb>, round: &RoundSnapshot) -> Result<Arc<Self>, String> {
        let mut state = DemoChain {
            tree: MemoryTreeServer::empty(),
            transactions: HashMap::new(),
            bodies: HashMap::new(),
            height: 1,
        };
        let rows = {
            let conn = db.conn();
            let mut stmt = conn.prepare("SELECT bundle_index, gov_comm, delegation_tx_hash FROM bundles WHERE wallet_id=?1 AND round_id=?2 ORDER BY bundle_index").map_err(|e|e.to_string())?;
            let result = stmt
                .query_map(rusqlite::params![db.wallet_id(), round.round_id], |r| {
                    Ok((
                        r.get::<_, u32>(0)?,
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            result
        };
        for (_, commitment, hash) in rows {
            let position = state.tree.size();
            append(&mut state.tree, &commitment)?;
            state.transactions.insert(
                hash,
                confirmed(
                    1,
                    event(
                        "delegate_vote",
                        &round.round_id,
                        vec![("leaf_index", position.to_string())],
                    ),
                ),
            );
        }
        state
            .tree
            .checkpoint(1)
            .map_err(|e| format!("demo checkpoint: {e:?}"))?;
        // Recreate the local ledger from confirmed public commitments after an
        // app restart or backup restore, preserving historical singleton layouts.
        let keys = {
            let conn = db.conn();
            let mut stmt=conn.prepare("SELECT bundle_index,proposal_id,tx_hash,vc_tree_position FROM votes WHERE wallet_id=?1 AND round_id=?2 AND vc_tree_position IS NOT NULL ORDER BY vc_tree_position").map_err(|e|e.to_string())?;
            let result = stmt
                .query_map(rusqlite::params![db.wallet_id(), round.round_id], |r| {
                    Ok((
                        r.get::<_, u32>(0)?,
                        r.get::<_, u32>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, u64>(3)?,
                    ))
                })
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            result
        };
        let mut batches = HashSet::new();
        for (bundle, proposal, hash, position) in keys {
            let recovery = recovery_bundle(&db, &round.round_id, bundle, proposal)
                .map_err(|e| e.to_string())?
                .ok_or("missing demo recovery")?;
            let recoveries = if let Some(batch) = &recovery.batch {
                if !batches.insert(batch.digest) {
                    continue;
                }
                let signed = zcash_voting::vote::recover_atomic_vote_batch(
                    &db,
                    &round.round_id,
                    bundle,
                    proposal,
                )
                .map_err(|e| e.to_string())?;
                signed
                    .commitments
                    .iter()
                    .map(|c| {
                        recovery_bundle(&db, &round.round_id, bundle, c.proposal_id)
                            .map_err(|e| e.to_string())?
                            .ok_or("missing batch recovery".into())
                    })
                    .collect::<Result<Vec<_>, String>>()?
            } else {
                vec![recovery]
            };
            if state.tree.size() + 1 != position {
                return Err("saved demo tree positions are inconsistent".into());
            }
            let receipt = state.apply_votes(&round.round_id, &recoveries)?;
            if let Some(hash) = hash {
                state.transactions.insert(hash, receipt);
            }
        }
        Ok(Arc::new(Self {
            db,
            round_id: round.round_id.clone(),
            state: Mutex::new(state),
        }))
    }

    fn read(&self, url: &str) -> Result<(u16, Value), String> {
        let state = self.state.lock().map_err(|_| "demo chain lock poisoned")?;
        if url.contains("/tx/") {
            return Ok(
                match state
                    .transactions
                    .get(url.rsplit('/').next().unwrap_or_default())
                {
                    Some(v) => (200, v.clone()),
                    None => (404, json!({"error":"tx not found"})),
                },
            );
        }
        if url.contains("/commitment-tree/") {
            if let Some(query) = url.split('?').nth(1) {
                let pairs = query
                    .split('&')
                    .filter_map(|v| v.split_once('='))
                    .collect::<HashMap<_, _>>();
                let from = pairs
                    .get("from_height")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                let to = pairs
                    .get("to_height")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(state.height);
                let page = state
                    .tree
                    .get_block_commitments(from, to)
                    .map_err(|e| e.to_string())?;
                return Ok((
                    200,
                    json!({"blocks":page.blocks.iter().map(|b|json!({"height":b.height,"start_index":b.start_index,"leaves":b.leaves.iter().map(|l|BASE64_STANDARD.encode(l.to_bytes())).collect::<Vec<_>>(),"root":BASE64_STANDARD.encode(b.root.to_repr())})).collect::<Vec<_>>(),"next_from_height":0}),
                ));
            }
            let height = url
                .rsplit('/')
                .next()
                .and_then(|v| v.parse::<u32>().ok())
                .unwrap_or(state.height);
            return Ok((
                200,
                json!({"tree":{"height":height,"next_index":state.tree.size(),"root":BASE64_STANDARD.encode(state.tree.root_at_height(height).unwrap_or_else(||state.tree.root()).to_repr())}}),
            ));
        }
        Err("unexpected offline chain route".into())
    }

    fn post(&self, body: Vec<u8>) -> Result<Value, String> {
        if let Some(response) = self
            .state
            .lock()
            .map_err(|_| "demo lock poisoned")?
            .bodies
            .get(&body)
        {
            return Ok(response.clone());
        }
        let value: Value = serde_json::from_slice(&body).map_err(|e| e.to_string())?;
        let votes = value
            .get("votes")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_else(|| vec![value.clone()]);
        let first = votes.first().ok_or("empty demo vote batch")?;
        let proposal = first["proposal_id"]
            .as_u64()
            .ok_or("demo vote has no proposal")? as u32;
        let nf = BASE64_STANDARD
            .decode(
                first["van_nullifier"]
                    .as_str()
                    .ok_or("demo vote has no nullifier")?,
            )
            .map_err(|e| e.to_string())?;
        let count = self
            .db
            .get_bundle_count(&self.round_id)
            .map_err(|e| e.to_string())?;
        let mut selected = None;
        for bundle in 0..count {
            if let Some(r) = recovery_bundle(&self.db, &self.round_id, bundle, proposal)
                .map_err(|e| e.to_string())?
            {
                if r.van_nullifier.as_slice() == nf {
                    selected = Some(bundle);
                    break;
                }
            }
        }
        let bundle = selected.ok_or("demo request does not match persisted recovery")?;
        let mut recoveries = Vec::new();
        for vote in votes {
            let proposal = vote["proposal_id"]
                .as_u64()
                .ok_or("invalid demo proposal")? as u32;
            recoveries.push(
                recovery_bundle(&self.db, &self.round_id, bundle, proposal)
                    .map_err(|e| e.to_string())?
                    .ok_or("missing demo vote recovery")?,
            );
        }
        let hash = hex::encode(Sha256::digest(&body));
        let mut state = self.state.lock().map_err(|_| "demo lock poisoned")?;
        let receipt = state.apply_votes(&self.round_id, &recoveries)?;
        state.transactions.insert(hash.clone(), receipt);
        let mut response = json!({"tx_hash":hash,"code":0});
        if let Some(batch) = recoveries.last().and_then(|r| r.batch.as_ref()) {
            response["batch_digest"] = json!(hex::encode(batch.digest));
        }
        state.bodies.insert(body, response.clone());
        Ok(response)
    }
}
impl DemoChain {
    fn apply_votes(&mut self, round: &str, votes: &[VoteRecoveryBundle]) -> Result<Value, String> {
        let last = votes.last().ok_or("empty demo batch")?;
        let van_position = self.tree.size();
        append(&mut self.tree, &last.vote_authority_note_new)?;
        for vote in votes {
            append(&mut self.tree, &vote.vote_commitment)?;
        }
        self.height += 1;
        self.tree
            .checkpoint(self.height)
            .map_err(|e| format!("demo checkpoint: {e:?}"))?;
        let event = if let Some(batch) = &last.batch {
            event(
                "cast_vote_batch",
                round,
                vec![
                    ("batch_digest", hex::encode(batch.digest)),
                    ("batch_size", votes.len().to_string()),
                    ("final_van_leaf_index", van_position.to_string()),
                    (
                        "vote_commitment_leaf_indices",
                        (1..=votes.len())
                            .map(|i| (van_position + i as u64).to_string())
                            .collect::<Vec<_>>()
                            .join(","),
                    ),
                    (
                        "proposal_ids",
                        votes
                            .iter()
                            .map(|v| v.proposal_id.to_string())
                            .collect::<Vec<_>>()
                            .join(","),
                    ),
                    (
                        "van_nullifiers",
                        votes
                            .iter()
                            .map(|v| hex::encode(v.van_nullifier))
                            .collect::<Vec<_>>()
                            .join(","),
                    ),
                ],
            )
        } else {
            event(
                "cast_vote",
                round,
                vec![(
                    "leaf_index",
                    format!("{},{}", van_position, van_position + 1),
                )],
            )
        };
        Ok(confirmed(self.height, event))
    }
}
impl ChainTransport for DemoTransport {
    fn chain_get<'a>(&'a self, request: ChainHttpRequest) -> ChainTransportFuture<'a> {
        Box::pin(async move {
            self.read(request.url())
                .map(|(status, v)| ChainHttpResponse::json(status, v.to_string().into_bytes()))
                .map_err(ChainTransportError::definitely_unsent)
        })
    }
    fn chain_post_json<'a>(
        &'a self,
        _request: ChainHttpRequest,
        body: Vec<u8>,
    ) -> ChainTransportFuture<'a> {
        Box::pin(async move {
            self.post(body)
                .map(|v| ChainHttpResponse::json(200, v.to_string().into_bytes()))
                .map_err(ChainTransportError::possibly_dispatched)
        })
    }
}
impl Transport for DemoTransport {
    fn get(&self, url: &str) -> Result<TransportResponse, TransportError> {
        self.read(url)
            .map(|(status, v)| TransportResponse {
                status,
                body: v.to_string().into_bytes(),
            })
            .map_err(TransportError::Request)
    }
}
pub struct DemoHelper;
impl HelperTransport for DemoHelper {
    fn get<'a>(&'a self, url: &'a str, _timeout: Duration) -> HelperFuture<'a> {
        Box::pin(async move {
            Ok(HelperResponse::json(
                200,
                json!({"status":if url.ends_with("/status") {"ok"} else {"confirmed"}})
                    .to_string()
                    .into_bytes(),
            ))
        })
    }
    fn post_json<'a>(
        &'a self,
        _url: &'a str,
        _body: Vec<u8>,
        _timeout: Duration,
    ) -> HelperFuture<'a> {
        Box::pin(async {
            Ok(HelperResponse::json(
                200,
                br#"{"status":"queued"}"#.to_vec(),
            ))
        })
    }
}
pub fn executor(
    db: Arc<VotingDb>,
    round: &RoundSnapshot,
    hotkey: &VotingHotkey,
) -> Result<(RoundExecutor<Arc<DemoTransport>>, Arc<DemoTransport>), String> {
    let transport = DemoTransport::new(Arc::clone(&db), round)?;
    let helpers = HelperClient::new(Arc::new(DemoHelper), HelperHealth::default());
    RoundExecutor::with_transport(
        db,
        Arc::clone(&transport),
        voter::chain_config(round),
        helpers,
    )
    .map_err(|e| e.to_string())?
    .with_binding(voter::round_binding(round, hotkey))
    .map(|e| (e.with_tree_transport(transport.clone()), transport))
    .map_err(|e| e.to_string())
}
pub async fn confirm_delegations(db: Arc<VotingDb>, round: &RoundSnapshot) -> Result<(), String> {
    let transport = DemoTransport::new(Arc::clone(&db), round)?;
    let client = ChainSubmissionClient::with_transport(
        Arc::clone(&db),
        transport,
        voter::chain_config(round),
    )
    .map_err(|e| e.to_string())?;
    for bundle_index in 0..db
        .get_bundle_count(&round.round_id)
        .map_err(|e| e.to_string())?
    {
        let result = client
            .advance_imported_delegation(
                AdvanceImportedDelegation {
                    vote_round_id: voter::round_bytes(round)?,
                    bundle_index,
                },
                &ChainSubmissionControl::new(0),
            )
            .await
            .map_err(|e| e.to_string())?;
        if !matches!(result, ChainSubmissionResult::Confirmed(_)) {
            return Err(format!("demo delegation not confirmed: {result:?}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::VoteChoiceInput;
    use zcash_voting::delegation_capability::{
        import_delegation_capability, ImportDelegationCapabilityParams,
    };

    fn import(db: &VotingDb, round: &RoundSnapshot, hotkey: &VotingHotkey) {
        import_delegation_capability(
            db,
            capability_json(hotkey).unwrap().as_bytes(),
            ImportDelegationCapabilityParams {
                voting_hotkey: hotkey,
                expected_chain_id: &round.chain_id,
                expected_network: round.profile.network(),
                expected_round_params: &round.params,
                session_json: None,
            },
        )
        .unwrap();
    }
    #[tokio::test]
    async fn imported_delegation_confirms_without_signing_or_broadcasting() {
        let round = round_snapshot();
        let db = Arc::new(VotingDb::open_in_memory().unwrap());
        db.set_wallet_id("demo-test");
        let hotkey = generate_random_voting_hotkey(Profile::Demo.network()).unwrap();
        import(&db, &round, &hotkey);
        confirm_delegations(Arc::clone(&db), &round).await.unwrap();
        let confirmed: u32 = db
            .conn()
            .query_row(
                "SELECT count(*) FROM bundles WHERE van_leaf_position IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(confirmed, 2);
        confirm_delegations(Arc::clone(&db), &round).await.unwrap();
        let transport = DemoTransport::new(Arc::clone(&db), &round).unwrap();
        assert!(transport.state.lock().unwrap().bodies.is_empty());
    }
    #[test]
    fn capability_rejects_wrong_hotkey_and_round() {
        let round = round_snapshot();
        let hotkey = generate_random_voting_hotkey(Profile::Demo.network()).unwrap();
        let other = generate_random_voting_hotkey(Profile::Demo.network()).unwrap();
        let db = VotingDb::open_in_memory().unwrap();
        db.set_wallet_id("negative-import-test");
        assert!(import_delegation_capability(
            &db,
            capability_json(&hotkey).unwrap().as_bytes(),
            ImportDelegationCapabilityParams {
                voting_hotkey: &other,
                expected_chain_id: &round.chain_id,
                expected_network: round.profile.network(),
                expected_round_params: &round.params,
                session_json: None,
            }
        )
        .is_err());
        let mut wrong = round.params.clone();
        wrong.vote_round_id = "ab".repeat(32);
        assert!(import_delegation_capability(
            &db,
            capability_json(&hotkey).unwrap().as_bytes(),
            ImportDelegationCapabilityParams {
                voting_hotkey: &hotkey,
                expected_chain_id: &round.chain_id,
                expected_network: round.profile.network(),
                expected_round_params: &wrong,
                session_json: None,
            }
        )
        .is_err());
        assert_eq!(db.get_bundle_count(&round.round_id).unwrap(), 0);
    }
    struct LostReply {
        inner: Arc<DemoTransport>,
        lost: std::sync::atomic::AtomicBool,
    }
    impl ChainTransport for LostReply {
        fn chain_get<'a>(&'a self, request: ChainHttpRequest) -> ChainTransportFuture<'a> {
            self.inner.chain_get(request)
        }
        fn chain_post_json<'a>(
            &'a self,
            request: ChainHttpRequest,
            body: Vec<u8>,
        ) -> ChainTransportFuture<'a> {
            Box::pin(async move {
                let response = self.inner.chain_post_json(request, body).await?;
                if !self.lost.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    Err(ChainTransportError::possibly_dispatched(
                        "simulated lost response after chain commit",
                    ))
                } else {
                    Ok(response)
                }
            })
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "generates real proofs and reconciles a landed batch whose POST response was lost"]
    async fn ambiguous_post_recovers_without_duplicate_commitments() {
        run_proof_recovery(false, true).await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "generates four real vote proofs and tests durable atomic-batch recovery"]
    async fn demo_generates_and_confirms_a_real_vote_proof() {
        run_proof_recovery(false, false).await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "generates real proofs, prevents expired POSTs, and resumes the same durable work"]
    async fn expired_transport_preserves_prepared_votes_for_recovery() {
        run_proof_recovery(true, false).await;
    }
    async fn run_proof_recovery(expired_first: bool, lost_reply: bool) {
        let root = std::env::temp_dir().join(format!("custody-v5-proof-{}", uuid::Uuid::new_v4()));
        let paths = crate::storage::profile_paths(&root, Profile::Demo).unwrap();
        let round = round_snapshot();
        let hotkey = generate_random_voting_hotkey(Profile::Demo.network()).unwrap();
        let choices = vec![
            VoteChoiceInput {
                proposal_id: 1,
                choice: 1,
            },
            VoteChoiceInput {
                proposal_id: 2,
                choice: 0,
            },
        ];
        let db = Arc::new(crate::storage::open_db(&paths, Profile::Demo).unwrap());
        import(&db, &round, &hotkey);
        let mut saved_proofs = Vec::new();
        if expired_first {
            let transport = DemoTransport::new(Arc::clone(&db), &round).unwrap();
            let executor = RoundExecutor::with_transport(
                Arc::clone(&db),
                voter::VotingWindowTransport {
                    inner: transport.clone(),
                    vote_end_time: 0,
                },
                voter::chain_config(&round),
                HelperClient::new(Arc::new(DemoHelper), HelperHealth::default()),
            )
            .unwrap()
            .with_binding(voter::round_binding(&round, &hotkey))
            .unwrap()
            .with_tree_transport(transport.clone());
            let report = voter::drive_round(
                &executor,
                &round,
                &choices,
                &hotkey,
                Some(transport.clone()),
                None,
            )
            .await
            .unwrap();
            assert!(!matches!(report.quiescence, RoundQuiescence::NoWorkLeft));
            assert!(
                transport.state.lock().unwrap().bodies.is_empty(),
                "expired transport must not POST"
            );
            let conn = db.conn();
            let mut stmt = conn.prepare("SELECT bundle_index,proposal_id,commitment_bundle_json FROM votes ORDER BY bundle_index,proposal_id").unwrap();
            saved_proofs = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, u32>(0)?,
                        r.get::<_, u32>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(saved_proofs.len(), 4);
        }
        let transport = DemoTransport::new(Arc::clone(&db), &round).unwrap();
        if lost_reply {
            let executor = RoundExecutor::with_transport(
                Arc::clone(&db),
                LostReply {
                    inner: transport.clone(),
                    lost: std::sync::atomic::AtomicBool::new(false),
                },
                voter::chain_config(&round),
                HelperClient::new(Arc::new(DemoHelper), HelperHealth::default()),
            )
            .unwrap()
            .with_binding(voter::round_binding(&round, &hotkey))
            .unwrap()
            .with_tree_transport(transport.clone());
            let _ = voter::drive_round(
                &executor,
                &round,
                &choices,
                &hotkey,
                Some(transport.clone()),
                None,
            )
            .await
            .unwrap();
        }
        let executor = RoundExecutor::with_transport(
            Arc::clone(&db),
            transport.clone(),
            voter::chain_config(&round),
            HelperClient::new(Arc::new(DemoHelper), HelperHealth::default()),
        )
        .unwrap()
        .with_binding(voter::round_binding(&round, &hotkey))
        .unwrap()
        .with_tree_transport(transport.clone());
        let report = voter::drive_round(
            &executor,
            &round,
            &choices,
            &hotkey,
            Some(transport.clone()),
            None,
        )
        .await
        .unwrap();
        assert!(report.failures.is_empty(), "{:?}", report.failures);
        assert!(
            matches!(
                report.quiescence,
                RoundQuiescence::BackgroundShareWorkOnly { .. } | RoundQuiescence::NoWorkLeft
            ),
            "{:?}",
            report.quiescence
        );
        for (bundle, proposal, proof) in saved_proofs {
            let current: String = db.conn().query_row("SELECT commitment_bundle_json FROM votes WHERE bundle_index=?1 AND proposal_id=?2", (bundle,proposal), |r| r.get(0)).unwrap();
            let mut original: Value = serde_json::from_str(&proof).unwrap();
            let mut resumed: Value = serde_json::from_str(&current).unwrap();
            // Confirmation adds the public position; all proof/signature/share
            // material must remain byte-for-byte equivalent.
            original.as_object_mut().unwrap().remove("vc_tree_position");
            resumed.as_object_mut().unwrap().remove("vc_tree_position");
            assert!(
                original == resumed,
                "retry must preserve proof and encrypted shares"
            );
        }
        let before = crate::storage::vote_records(&paths, Profile::Demo, &round.round_id).unwrap();
        assert_eq!(before.len(), 4);
        assert!(before
            .iter()
            .all(|v| (lost_reply || v.tx_hash.is_some()) && v.vc_tree_position.is_some()));
        assert_eq!(
            transport.state.lock().unwrap().tree.size(),
            8,
            "two delegation leaves and two batches, without duplicates"
        );
        for bundle in 0..2 {
            let votes = before
                .iter()
                .filter(|v| v.bundle_index == bundle)
                .collect::<Vec<_>>();
            assert_eq!(
                votes[0].tx_hash, votes[1].tx_hash,
                "one atomic tx per bundle"
            );
        }
        let helpers = HelperClient::new(Arc::new(DemoHelper), HelperHealth::default());
        let host = zcash_voting::share_tracking_drive::ShareTrackingHostSourceBridge::new(|| {
            ShareTrackingHostContext {
                configured_helper_urls: voter::server_urls(&round),
                now_seconds: round.vote_end_time - 1,
                vote_end_time_seconds: Some(round.vote_end_time),
            }
        });
        let tracked = ShareTrackingDriver::new(&db, &helpers, &round.round_id)
            .with_policy(ShareTrackingDrivePolicy {
                max_passes: Some(1),
                ..Default::default()
            })
            .run(
                &host,
                &ChainSubmissionControl::new(0),
                &NoopShareTrackingReporter::default(),
            )
            .await;
        assert!(tracked.failures.is_empty(), "{:?}", tracked.failures);
        let progress = crate::storage::round_progress(
            &paths,
            Profile::Demo,
            &round.round_id,
            true,
            true,
            &round.vote_servers,
        )
        .unwrap();
        assert_eq!(
            progress.confirmed_share_count,
            progress.required_share_count
        );
        drop(executor);
        drop(db);
        let db = Arc::new(crate::storage::open_db(&paths, Profile::Demo).unwrap());
        let (executor, tree) = super::executor(Arc::clone(&db), &round, &hotkey).unwrap();
        let report = voter::drive_round(&executor, &round, &choices, &hotkey, Some(tree), None)
            .await
            .unwrap();
        assert!(report.failures.is_empty(), "{:?}", report.failures);
        assert!(
            matches!(report.quiescence, RoundQuiescence::NoWorkLeft),
            "{:?}",
            report.quiescence
        );
        assert_eq!(
            before,
            crate::storage::vote_records(&paths, Profile::Demo, &round.round_id).unwrap()
        );
        drop(executor);
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
}
