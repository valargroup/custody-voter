# Zcash Custody Voter

Zcash Custody Voter is the customer-side application for custody-delegated Zcash voting. A custodian keeps control of the customer's funds while the customer creates and controls a separate voting hotkey, imports the custodian's delegation capability, and casts private votes.

The app supports Mainnet and Testnet. Development and debug builds also include a self-contained Local Demo. It is a Tauri desktop application so voting secrets and proof generation remain in native Rust rather than browser storage or JavaScript.

## Build instruction for AI agents

When asked to build the application, produce a production-optimized release build by default with `npm run tauri build`. A development server, frontend-only build, or Tauri debug build does not satisfy an unqualified build request. Use a debug build only when the requester explicitly asks for one, and confirm that the completed app bundle came from `src-tauri/target/release/bundle`, not `src-tauri/target/debug/bundle`. Production release builds omit the Local Demo selector and the Testnet custodian simulator, including its native provider-side implementation.

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

## Test the custody handoff with a real Testnet wallet

Development and debug builds include an intentionally separate **local integration harness** in the Testnet profile. It lets a developer temporarily perform the custodian side of the protocol, broadcast the real delegation transactions to the Stage vote chain, and produce the exact canonical JSON that the customer workflow imports. Production release builds exclude both the harness UI and its native implementation, so the customer flow never asks for a wallet seed.

You need:

- an authenticated Testnet round whose status is **Active**;
- a disposable Testnet wallet mnemonic whose ZIP-32 account 0 had at least one ballot of eligible Orchard weight at the round snapshot;
- a wallet birthday block height at or before that account's earliest transaction; and
- access to the configured Stage vote-chain and PIR services plus a Testnet lightwalletd endpoint.

Then:

1. Open **Testnet**, choose the active round, and select **Generate customer target**.
2. Under **Import the custody payload**, open **Generate this payload with a real Testnet wallet**.
3. Enter the account's wallet birthday height and mnemonic. ZIP-32 account 0 is selected automatically.
4. Acknowledge the Testnet warning and select **Recover, build, and broadcast**. The app creates an isolated wallet database and syncs it through the round snapshot with the configured lightwalletd. The default endpoint is `https://testnet.zec.rocks:443` and can be changed in the form.
5. Keep the app open while it syncs and creates the delegation proofs. The mnemonic is held only for this operation and cleared from the form afterward.
6. When the exact payload is ready, it is placed in the normal customer import field. Select **Verify and import**, wait for confirmation if needed, choose each proposal response, and cast the vote.

This broadcasts real governance transactions, but the synthetic delegation PCZT is not a spendable Zcash transaction and no Testnet ZEC is moved or consumed. Use a disposable Testnet wallet anyway because its mnemonic temporarily enters this development build.

The operation is recoverable. Each scan batch is committed to the isolated wallet, and before broadcasting the app durably stores the exact signed vote-chain bytes and canonical capability. If it is interrupted, enter the same mnemonic and birthday and run the action again. Accepted bundles are detected or replayed byte-for-byte rather than rebuilt. The isolated wallet database is deleted once the signed capability is safely persisted; an incomplete database remains in the app's private data directory solely so the same job can resume.

## Customer workflow

1. Choose the correct network and voting round.
2. Generate a public voting target. The app creates one independent voting hotkey for that network and round and stores it in the operating-system Keychain.
3. Export an encrypted recovery backup before handing off the target.
4. Send only the downloaded public target JSON to the custody provider.
5. Import the exact capability JSON returned by the custody provider.
6. Wait until every named delegation transaction is confirmed.
7. Review the network, round, and one selection per proposal.
8. Generate, sign, submit, and confirm the votes. If the app is interrupted, start the same action again to recover the persisted signed vote instead of producing a conflicting one.

Mainnet and Testnet are separate profiles with separate databases, manifests, and Keychain namespaces. Development and debug builds default to Local Demo. Production release builds omit that selector and default to Testnet. Mainnet always has a persistent production warning.

## Security model

- The hotkey is generated with the operating system's cryptographic RNG and never crosses the Tauri command boundary into React.
- Public targets are bound to a chain, network, round, and Orchard address.
- Custody capabilities are accepted only through the strict canonical `zcash_voting` importer. Network, round parameters, target address, bundle ordering, and transaction hashes must match.
- Mainnet and Testnet service discovery begins from checksum-pinned static configuration. Dynamic configuration and round parameters are authenticated by `zcash_voting` before use.
- Voting is blocked until every imported delegation has an on-chain VAN position.
- Signed vote recovery is persisted before network submission, so an uncertain or interrupted submission can resume safely.
- Development and debug builds include a Testnet custodian harness that recovers ZIP-32 account 0 from a supplied mnemonic and birthday into a private SQLite database, syncs only through the authenticated round snapshot, and persists signed delegation bytes before broadcast. The harness is excluded from production release builds.
- HTTP responses are bounded while streaming. A helper share is recorded only after the configured helper redundancy target accepts it.
- Recovery backups use passphrase-based age encryption and contain only voting state and voting hotkeys. They never contain custody funds, wallet seeds, or mnemonics.

Test custodian jobs are deliberately excluded from customer recovery backups. They can contain privacy-sensitive recovered-wallet data and provider-side proof state, so use the harness only with disposable Testnet wallets and remove the app's local data when the rehearsal is no longer needed.

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

The live Testnet wallet-recovery smoke test is also opt-in:

```sh
cd src-tauri
cargo test test_custodian::tests::recovers_account_zero_and_scans_real_testnet_blocks -- --ignored --exact
```

## Signed macOS releases

The `Signed macOS release` GitHub Actions workflow builds the exact commit named by a version tag as a production-optimized universal macOS application. It signs the app with Developer ID, submits it to Apple's notarization service, staples the notarization ticket, verifies both the app and DMG with Apple's command-line tools, and then publishes these release assets:

- `Custody-Voter-macos.dmg`
- `Custody-Voter-macos.dmg.sha256`

Pushing a tag such as `v1.2.3` starts the workflow. The tag must exactly match `v` plus the version in `src-tauri/tauri.conf.json`. An active repository ruleset limits creating, moving, or deleting matching release tags to the designated release operator. The build also targets the protected `macos-release` environment, which accepts only version tags and requires approval before exposing its secrets. If a run is interrupted, rerun the original tag-triggered workflow from GitHub Actions; it resumes an existing draft. Do not add a branch-selectable manual trigger because that would let untrusted workflow code request the signing environment.

The signing job has read-only repository permissions and does not persist checkout credentials. It passes the verified DMG and checksum through GitHub's artifact service to a separate publication job. Only that publication job has `contents: write`; it has no Apple secrets and does not check out or execute repository code. The workflow rechecks that the remote tag still names the built commit before touching a release and again immediately before publication. Versions with a prerelease suffix are published as prereleases, and GitHub determines which stable release is latest. Assets are uploaded to a draft first and the release is published only after both uploads succeed. Already-published releases and their assets are left unchanged. Release-asset visibility follows the repository's GitHub visibility.

The protected `macos-release` environment requires these encrypted Actions secrets. Keep them environment-scoped; repository-scoped copies would be available to unrelated workflows.

- `APPLE_CERTIFICATE`: base64-encoded PKCS #12 archive containing the Developer ID Application certificate and its private key
- `APPLE_CERTIFICATE_PASSWORD`: export password for that PKCS #12 archive
- `APPLE_ID`: Apple Account email used for notarization
- `APPLE_PASSWORD`: a dedicated Apple app-specific password, never the normal Apple Account password
- `APPLE_TEAM_ID`: Apple Developer Program team identifier

The certificate is imported into a temporary CI keychain and removed at the end of the job. Keep the original signing key in the macOS Keychain, revoke the app-specific password if CI access is no longer needed, and revoke the Developer ID certificate immediately if its private key might have been exposed. Do not place any of these values in repository files or workflow logs.

After downloading a release on macOS, its checksum and Apple assessment can be checked with:

```sh
shasum -a 256 -c Custody-Voter-macos.dmg.sha256
spctl --assess --type open --context context:primary-signature --verbose=2 Custody-Voter-macos.dmg
xcrun stapler validate Custody-Voter-macos.dmg
```

## Current scope

- The first release is a desktop app. There is no hosted web version because a browser would put voting-secret storage and native proof dependencies behind a weaker boundary.
- Mainnet and Testnet use the currently pinned Valar voting configuration revisions. Updating those trust anchors is an intentional source change and release event.
- GitHub release DMGs are signed and notarized for direct macOS distribution. An automatic update channel remains future deployment work.

## License

Licensed under either of the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT license](LICENSE-MIT), at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this project is dual licensed as above, without any additional
terms or conditions.
