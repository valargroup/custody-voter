# Architecture

## Why a desktop app

The customer tool owns a small but security-sensitive secret and must run the native Zcash voting proof stack. Tauri keeps the presentation layer lightweight while placing hotkey generation, capability validation, persistence, networking, proof generation, signing, and recovery in Rust. A Vizor fork would inherit wallet and spending-key scope that this customer tool does not need. A hosted browser app would introduce browser storage and WebAssembly constraints at the most sensitive boundary.

```mermaid
flowchart LR
    UI["React customer workflow"] -->|"public inputs only"| IPC["Narrow Tauri commands"]
    IPC --> CORE["Rust voting coordinator"]
    CORE --> SDK["zcash_voting"]
    CORE --> DB["Profile SQLite database"]
    CORE --> KC["Operating-system Keychain"]
    CORE --> CFG["Pinned authenticated config"]
    CFG --> CHAIN["Vote-chain and helper services"]
    CUST["Custody provider"] -->|"canonical capability JSON"| UI
    UI -->|"public target JSON"| CUST
```

The hotkey travels only between `zcash_voting`, zeroizing Rust buffers, and the operating-system Keychain. It is deliberately absent from every command response and frontend type.

## Profile isolation

Each profile has an independent directory, database wallet identifier, manifest, and Keychain service:

| Profile | Zcash network | Vote chain | Local directory |
| --- | --- | --- | --- |
| Mainnet | Mainnet | `zvote-1` | `mainnet/` |
| Testnet | Testnet | `svote-1` | `testnet/` |
| Local Demo | Regtest | `custody-voter-demo-1` | `demo/` |

Commands always receive an explicit profile. The backend checks that the profile, chain identifier, network, round identifier, and trusted round parameters agree before acting.

## State transitions

```mermaid
stateDiagram-v2
    [*] --> RoundSelected
    RoundSelected --> TargetReady: Generate hotkey and public target
    TargetReady --> CapabilityImported: Strict canonical import
    CapabilityImported --> DelegationsConfirmed: Confirm every custody transaction
    DelegationsConfirmed --> VotePersisted: Build proof and sign locally
    VotePersisted --> VoteSubmitted: Broadcast signed payload
    VoteSubmitted --> VoteConfirmed: Parse on-chain positions
    VoteConfirmed --> SharesSubmitted: Meet helper redundancy policy
    VotePersisted --> VotePersisted: Recover after interruption
    VoteSubmitted --> VoteSubmitted: Poll after uncertain confirmation
```

The database is the recovery source of truth after a vote commitment is built. A retry never rebuilds a commitment with a different selection under the same bundle and proposal key.

## Network authentication

Mainnet and Testnet each begin with a source URL and SHA-256 checksum embedded in `chain.rs`. `zcash_voting` verifies that static document, follows and authenticates its dynamic configuration, and constructs trusted round parameters with the authenticated election-authority key. Rounds not named by the resolved configuration are excluded.

All remote requests run in Rust with TLS, connect and total timeouts, failover across configured endpoints, response-size limits enforced while streaming, and no WebView network permission.

## Backup and restore

An export checkpoints SQLite, reads every round hotkey from Keychain, serializes one profile, and encrypts the envelope with age scrypt passphrase encryption. Sensitive serialized fields are zeroized when the envelope is dropped.

Restore performs these checks before activation:

1. age decryption and format/profile validation;
2. an exact, duplicate-free match between manifest rounds and hotkeys;
3. SQLite parsing, integrity checking, network checks, and rejection of unexpected rounds;
4. reconstruction of every hotkey and exact regeneration of its stored public target;
5. capture of the current Keychain state for rollback;
6. Keychain replacement and atomic database/manifest activation.

Only the selected profile is replaced. If file activation fails, the prior Keychain state is restored.
