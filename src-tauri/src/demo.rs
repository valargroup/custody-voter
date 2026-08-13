use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use ff::PrimeField;
use group::{Group, GroupEncoding};
use pasta_curves::{pallas, Fp};
use rusqlite::named_params;
use sha2::{Digest, Sha256};
use vote_commitment_tree::MemoryTreeServer;
use zcash_voting::{
    confirmation::{
        confirm_delegation_submission, confirm_vote_submission, TxEvent, TxEventAttribute,
    },
    delegation_capability::{DelegationCapabilityBundleV1, DelegationCapabilityV1},
    round::VotingDb,
    vote::{recover_signed_commitments, SignedVoteCommitment, VanWitness},
    VotingHotkey,
};

use crate::model::{AppOption, AppProposal, Profile, RoundSnapshot};

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

pub fn confirm_delegations(db: &VotingDb, round_id: &str) -> Result<(), String> {
    let wallet = db.wallet_id();
    let rows = {
        let conn = db.conn();
        let mut statement = conn
            .prepare(
                "SELECT bundle_index, delegation_tx_hash
                 FROM bundles
                 WHERE round_id = :round_id AND wallet_id = :wallet_id
                 ORDER BY bundle_index",
            )
            .map_err(|error| format!("prepare demo delegation query failed: {error}"))?;
        let mapped = statement
            .query_map(
                named_params! { ":round_id": round_id, ":wallet_id": wallet },
                |row| Ok((row.get::<_, u32>(0)?, row.get::<_, String>(1)?)),
            )
            .map_err(|error| format!("query demo delegations failed: {error}"))?;
        mapped
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("decode demo delegation failed: {error}"))?
    };
    if rows.is_empty() {
        return Err("import the demo custody payload before confirming it".to_string());
    }
    for (bundle_index, tx_hash) in rows {
        let events = vec![event("delegate_vote", round_id, bundle_index.to_string())];
        confirm_delegation_submission(db, round_id, bundle_index, &tx_hash, &events)
            .map_err(|error| format!("confirm demo delegation failed: {error}"))?;
    }
    Ok(())
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

pub fn confirm_vote(
    db: &VotingDb,
    round_id: &str,
    bundle_index: u32,
    commitment: &SignedVoteCommitment,
) -> Result<(String, u64), String> {
    let (tree, _) = rebuild_tree(db, round_id)?;
    let van_position = tree.size();
    let vc_position = van_position + 1;
    let tx_hash = hex::encode(Sha256::digest(
        [
            commitment.vote_authority_note_new.as_slice(),
            commitment.vote_commitment.as_slice(),
        ]
        .concat(),
    ));
    let events = vec![event(
        "cast_vote",
        round_id,
        format!("{van_position},{vc_position}"),
    )];
    confirm_vote_submission(
        db,
        round_id,
        bundle_index,
        commitment.proposal_id,
        &tx_hash,
        &events,
    )
    .map_err(|error| format!("confirm demo vote failed: {error}"))?;
    Ok((tx_hash, vc_position))
}

fn rebuild_tree(
    db: &VotingDb,
    round_id: &str,
) -> Result<(MemoryTreeServer, Vec<(u32, u32)>), String> {
    let wallet = db.wallet_id();
    let (initial_vans, current_positions, vote_keys) = {
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

fn event(event_type: &str, round_id: &str, leaf_index: String) -> TxEvent {
    TxEvent {
        event_type: event_type.to_string(),
        attributes: vec![
            TxEventAttribute {
                key: "vote_round_id".to_string(),
                value: round_id.to_string(),
            },
            TxEventAttribute {
                key: "leaf_index".to_string(),
                value: leaf_index,
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zcash_voting::{
        delegation_capability::{import_delegation_capability, ImportDelegationCapabilityParams},
        hotkey::generate_random_voting_hotkey,
        vote::{commit_batch, DraftVote, VoteSigner},
        NoopProgressReporter,
    };

    #[test]
    fn demo_capability_uses_real_atomic_import_and_confirmation() {
        let round = round_snapshot();
        let hotkey = generate_random_voting_hotkey(Profile::Demo.network()).unwrap();
        let capability = capability_json(&hotkey).unwrap();
        let db = VotingDb::open_in_memory().unwrap();
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
        assert_eq!(db.get_bundle_count(&round.round_id).unwrap(), 2);
        confirm_delegations(&db, &round.round_id).unwrap();
        let witness = witness(&db, &round.round_id, 1).unwrap();
        assert_eq!(witness.position, 1);
        assert_eq!(witness.auth_path.len(), 24);
    }

    #[test]
    #[ignore = "generates a full zero-knowledge vote proof"]
    fn demo_generates_and_confirms_a_real_vote_proof() {
        let round = round_snapshot();
        let hotkey = generate_random_voting_hotkey(Profile::Demo.network()).unwrap();
        let capability = capability_json(&hotkey).unwrap();
        let db = VotingDb::open_in_memory().unwrap();
        db.set_wallet_id("demo-proof-test");
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
        confirm_delegations(&db, &round.round_id).unwrap();
        let witness = witness(&db, &round.round_id, 0).unwrap();
        let signed = commit_batch(
            &db,
            &round.round_id,
            0,
            &[DraftVote {
                proposal_id: 1,
                choice: 1,
                num_options: 3,
                vc_tree_position: 0,
                single_share: true,
            }],
            &witness,
            VoteSigner::hotkey(&hotkey),
            &NoopProgressReporter,
        )
        .unwrap();
        let commitment = signed.commitments.first().unwrap();
        let (tx_hash, vc_position) = confirm_vote(&db, &round.round_id, 0, commitment).unwrap();
        assert_eq!(tx_hash.len(), 64);
        assert_eq!(vc_position, 3);
    }
}
