# Valar Custody Voter

Valar Custody Voter is the customer-side application for custody-delegated Zcash voting. A custodian keeps control of the customer's funds while the customer creates and controls a separate voting hotkey, imports the custodian's delegation capability, and casts private votes.

The app supports Mainnet, Testnet, and a self-contained Local Demo. It is a Tauri desktop application so voting secrets and proof generation remain in native Rust rather than browser storage or JavaScript.

## Try the complete flow locally

Prerequisites:

- macOS with Xcode Command Line Tools
- the current stable Rust toolchain
- Node.js 20.19 or newer

From the repository root:

```sh
npm install
npm run tauri dev
```

The app opens in **Local Demo**, which does not use the network. To exercise the whole customer journey:

1. Select **Generate customer target**.
2. Select **Simulate custodian response**.
3. Select **Verify and import**.
4. Select **Simulate confirmations**.
5. Choose one option for each proposal and check the review box.
6. Select **Run local proof rehearsal** and keep the app open while it produces the real zero-knowledge proofs.

The demo uses the production capability parser, voting database, Merkle witnesses, proof builder, hotkey signing, persisted recovery state, and confirmation parser. Delegation and vote-chain confirmations are generated locally, and nothing is broadcast.

To clear the rehearsal, select **Reset local demo** in the sidebar. This removes only the Local Demo database and demo hotkey.

## Customer workflow

1. Choose the correct network and voting round.
2. Generate a public voting target. The app creates one independent voting hotkey for that network and round and stores it in the operating-system Keychain.
3. Export an encrypted recovery backup before handing off the target.
4. Send only the downloaded public target JSON to the custody provider.
5. Import the exact capability JSON returned by the custody provider.
6. Wait until every named delegation transaction is confirmed.
7. Review the network, round, and one selection per proposal.
8. Generate, sign, submit, and confirm the votes. If the app is interrupted, start the same action again to recover the persisted signed vote instead of producing a conflicting one.

Mainnet and Testnet are separate profiles with separate databases, manifests, and Keychain namespaces. The UI defaults to Local Demo and gives Mainnet a persistent production warning.

## Security model

- The hotkey is generated with the operating system's cryptographic RNG and never crosses the Tauri command boundary into React.
- Public targets are bound to a chain, network, round, and Orchard address.
- Custody capabilities are accepted only through the strict canonical `zcash_voting` importer. Network, round parameters, target address, bundle ordering, and transaction hashes must match.
- Mainnet and Testnet service discovery begins from checksum-pinned static configuration. Dynamic configuration and round parameters are authenticated by `zcash_voting` before use.
- Voting is blocked until every imported delegation has an on-chain VAN position.
- Signed vote recovery is persisted before network submission, so an uncertain or interrupted submission can resume safely.
- HTTP responses are bounded while streaming. A helper share is recorded only after the configured helper redundancy target accepts it.
- Recovery backups use passphrase-based age encryption and contain only voting state and voting hotkeys. They never contain custody funds, wallet seeds, or mnemonics.

See [SECURITY.md](SECURITY.md) and [docs/architecture.md](docs/architecture.md) for trust boundaries and recovery details.

## Development checks

```sh
npm run build
cd src-tauri
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --lib
```

The full proof smoke test is ignored during ordinary CI because it is CPU intensive. Run it explicitly with:

```sh
cd src-tauri
cargo test demo::tests::demo_generates_and_confirms_a_real_vote_proof -- --ignored --exact
```

## Current scope

- The first release is a desktop app. There is no hosted web version because a browser would put voting-secret storage and native proof dependencies behind a weaker boundary.
- Mainnet and Testnet use the currently pinned Valar voting configuration revisions. Updating those trust anchors is an intentional source change and release event.
- Distribution signing, notarization, and an update channel are deployment work. Local development and unsigned local bundles work without them.
