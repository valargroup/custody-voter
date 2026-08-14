import { useCallback, useEffect, useRef, useState } from "react";
import { listen } from "@tauri-apps/api/event";
import { api, errorMessage } from "./api";
import type {
  CastVotesResult,
  Profile,
  RoundCard,
  RoundWorkspace,
  TestCustodianProgressEvent,
  TestCustodianResult,
  VoteProgressEvent,
} from "./types";
import appIconUrl from "../src-tauri/icons/icon.png";
import "./App.css";

const DEFAULT_TESTNET_LIGHTWALLETD = "https://testnet.zec.rocks:443";

const PROFILES: Array<{ id: Profile; label: string; eyebrow: string }> = [
  { id: "demo", label: "Local Demo", eyebrow: "Offline rehearsal" },
  { id: "testnet", label: "Testnet", eyebrow: "Stage vote chain" },
  { id: "mainnet", label: "Mainnet", eyebrow: "Production" },
];

type BusyAction =
  | "rounds"
  | "workspace"
  | "target"
  | "capability"
  | "custodian"
  | "confirmation"
  | "vote"
  | "backup"
  | "restore"
  | "reset"
  | null;

type BackupMode = "export" | "restore" | null;

function App() {
  const [profile, setProfile] = useState<Profile>("demo");
  const [rounds, setRounds] = useState<RoundCard[]>([]);
  const [selectedRoundId, setSelectedRoundId] = useState<string | null>(null);
  const [workspace, setWorkspace] = useState<RoundWorkspace | null>(null);
  const [busy, setBusy] = useState<BusyAction>(null);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [capabilityText, setCapabilityText] = useState("");
  const [capabilityBytes, setCapabilityBytes] = useState<Uint8Array | null>(null);
  const [capabilityFilename, setCapabilityFilename] = useState<string | null>(null);
  const [choices, setChoices] = useState<Record<number, number>>({});
  const [reviewed, setReviewed] = useState(false);
  const [voteProgress, setVoteProgress] = useState<VoteProgressEvent | null>(null);
  const [voteResult, setVoteResult] = useState<CastVotesResult | null>(null);
  const [backupMode, setBackupMode] = useState<BackupMode>(null);
  const [passphrase, setPassphrase] = useState("");
  const [confirmPassphrase, setConfirmPassphrase] = useState("");
  const [restoreBytes, setRestoreBytes] = useState<Uint8Array | null>(null);
  const [restoreFilename, setRestoreFilename] = useState<string | null>(null);
  const [custodianOpen, setCustodianOpen] = useState(false);
  const [custodianBirthdayHeight, setCustodianBirthdayHeight] = useState("");
  const [custodianMnemonic, setCustodianMnemonic] = useState("");
  const [showCustodianMnemonic, setShowCustodianMnemonic] = useState(false);
  const [lightwalletdUrl, setLightwalletdUrl] = useState(DEFAULT_TESTNET_LIGHTWALLETD);
  const [custodianAcknowledged, setCustodianAcknowledged] = useState(false);
  const [custodianProgress, setCustodianProgress] = useState<TestCustodianProgressEvent | null>(null);
  const [custodianResult, setCustodianResult] = useState<TestCustodianResult | null>(null);
  const selectedRef = useRef<string | null>(null);
  const roundsRequestRef = useRef(0);
  const workspaceRequestRef = useRef(0);

  selectedRef.current = selectedRoundId;

  useEffect(() => {
    let unlisten: (() => void) | undefined;
    void listen<VoteProgressEvent>("vote-progress", ({ payload }) => {
      if (payload.roundId === selectedRef.current) setVoteProgress(payload);
    }).then((stop) => {
      unlisten = stop;
    });
    return () => unlisten?.();
  }, []);

  useEffect(() => {
    let unlisten: (() => void) | undefined;
    void listen<TestCustodianProgressEvent>("custodian-progress", ({ payload }) => {
      if (payload.roundId === selectedRef.current) setCustodianProgress(payload);
    }).then((stop) => {
      unlisten = stop;
    });
    return () => unlisten?.();
  }, []);

  const loadWorkspace = useCallback(
    async (roundId: string, currentProfile = profile) => {
      const requestId = ++workspaceRequestRef.current;
      setBusy("workspace");
      setError(null);
      try {
        const next = await api.getWorkspace(currentProfile, roundId);
        if (requestId !== workspaceRequestRef.current) return null;
        setWorkspace(next);
        setChoices(existingChoices(next));
        setReviewed(false);
        return next;
      } catch (caught) {
        if (requestId !== workspaceRequestRef.current) return null;
        setError(errorMessage(caught));
        setWorkspace(null);
        return null;
      } finally {
        if (requestId === workspaceRequestRef.current) setBusy(null);
      }
    },
    [profile],
  );

  const loadRounds = useCallback(
    async (currentProfile: Profile, preferredRoundId?: string | null) => {
      const requestId = ++roundsRequestRef.current;
      workspaceRequestRef.current += 1;
      setBusy("rounds");
      setError(null);
      try {
        const next = await api.listRounds(currentProfile);
        if (requestId !== roundsRequestRef.current) return;
        setRounds(next);
        const preferred = preferredRoundId
          ? next.find((round) => round.roundId === preferredRoundId)
          : undefined;
        const selected = preferred ?? next.find((round) => round.stored) ?? next[0];
        setSelectedRoundId(selected?.roundId ?? null);
        if (selected) {
          await loadWorkspace(selected.roundId, currentProfile);
        } else {
          setWorkspace(null);
        }
      } catch (caught) {
        if (requestId !== roundsRequestRef.current) return;
        setRounds([]);
        setSelectedRoundId(null);
        setWorkspace(null);
        setError(errorMessage(caught));
      } finally {
        if (requestId === roundsRequestRef.current) setBusy(null);
      }
    },
    [loadWorkspace],
  );

  useEffect(() => {
    setRounds([]);
    setSelectedRoundId(null);
    setWorkspace(null);
    setCapabilityText("");
    setCapabilityBytes(null);
    setCapabilityFilename(null);
    setVoteResult(null);
    setVoteProgress(null);
    resetCustodianForm();
    setNotice(null);
    void loadRounds(profile);
  }, [profile]); // eslint-disable-line react-hooks/exhaustive-deps

  const refreshAll = async (message?: string) => {
    await loadRounds(profile, selectedRoundId);
    if (message) setNotice(message);
  };

  const selectRound = async (roundId: string) => {
    roundsRequestRef.current += 1;
    setSelectedRoundId(roundId);
    setCapabilityText("");
    setCapabilityBytes(null);
    setCapabilityFilename(null);
    setVoteResult(null);
    setVoteProgress(null);
    resetCustodianForm();
    setNotice(null);
    await loadWorkspace(roundId);
  };

  const runAction = async <T,>(action: BusyAction, operation: () => Promise<T>) => {
    setBusy(action);
    setError(null);
    setNotice(null);
    try {
      return await operation();
    } catch (caught) {
      setError(errorMessage(caught));
      return undefined;
    } finally {
      setBusy(null);
    }
  };

  const generateTarget = async () => {
    if (!workspace) return;
    const result = await runAction("target", () =>
      api.generateTarget(profile, workspace.round.roundId),
    );
    if (!result) return;
    await refreshAll(
      result.reused
        ? "Existing customer target verified against the Keychain hotkey."
        : "Customer hotkey generated and stored in the operating-system Keychain.",
    );
  };

  const generateDemoPayload = async () => {
    if (!workspace) return;
    const text = await runAction("capability", () =>
      api.generateDemoCapability(workspace.round.roundId),
    );
    if (!text) return;
    setCapabilityText(text);
    setCapabilityBytes(new TextEncoder().encode(text));
    setCapabilityFilename("demo-delegation-capability.json");
    setNotice("Sample custody payload generated. Import it below exactly as provided.");
  };

  const resetCustodianForm = () => {
    setCustodianOpen(false);
    setCustodianBirthdayHeight("");
    setCustodianMnemonic("");
    setShowCustodianMnemonic(false);
    setLightwalletdUrl(DEFAULT_TESTNET_LIGHTWALLETD);
    setCustodianAcknowledged(false);
    setCustodianProgress(null);
    setCustodianResult(null);
  };

  const generateTestnetPayload = async () => {
    if (!workspace) return;
    const birthdayHeight = Number(custodianBirthdayHeight);
    if (!Number.isSafeInteger(birthdayHeight)) return;
    setCustodianProgress({
      roundId: workspace.round.roundId,
      phase: "starting",
      bundleIndex: null,
      bundleCount: null,
      progress: null,
      message: "Starting the recoverable Testnet custody job",
    });
    const result = await runAction("custodian", () =>
      api
        .generateTestnetCustodyPayload(
          workspace.round.roundId,
          birthdayHeight,
          custodianMnemonic,
          lightwalletdUrl,
        )
        .finally(() => setCustodianMnemonic("")),
    );
    if (!result) return;
    setCustodianResult(result);
    setCapabilityFromText(result.capabilityJson);
    setCapabilityFilename(
      `testnet-custody-capability-${workspace.round.roundId.slice(0, 10)}.json`,
    );
    setNotice(
      `${result.submittedBundleCount} Testnet delegation transaction${result.submittedBundleCount === 1 ? " was" : "s were"} accepted. The exact customer payload is ready below.`,
    );
  };

  const importCapability = async () => {
    if (!workspace || !capabilityBytes) return;
    const result = await runAction("capability", () =>
      api.importCapability(profile, workspace.round.roundId, capabilityBytes),
    );
    if (!result) return;
    await refreshAll(
      result.pendingTransactionHashes.length
        ? `Custody payload verified. ${result.pendingTransactionHashes.length} delegation transaction(s) are still pending.`
        : "Custody payload verified and every delegation is confirmed.",
    );
  };

  const checkConfirmations = async () => {
    if (!workspace) return;
    const result = await runAction("confirmation", () =>
      api.checkDelegations(profile, workspace.round.roundId),
    );
    if (!result) return;
    await refreshAll(
      result.pendingTransactionHashes.length
        ? `${result.pendingTransactionHashes.length} delegation transaction(s) are still pending.`
        : "Every custody delegation is confirmed. This round is ready to vote.",
    );
  };

  const castVotes = async () => {
    if (!workspace) return;
    setVoteResult(null);
    setVoteProgress({
      roundId: workspace.round.roundId,
      phase: "starting",
      bundleIndex: null,
      proposalId: null,
      progress: null,
      message: "Starting the protected vote flow",
    });
    const result = await runAction("vote", () =>
      api.castVotes(
        profile,
        workspace.round.roundId,
        workspace.round.proposals.map((proposal) => ({
          proposalId: proposal.id,
          choice: choices[proposal.id],
        })),
      ),
    );
    if (!result) return;
    setVoteResult(result);
    await refreshAll(
      result.demo
        ? "The local rehearsal completed with real proofs. No transaction left this computer."
        : "Every vote was confirmed and its encrypted helper shares were submitted.",
    );
  };

  const handleCapabilityFile = async (file: File | undefined) => {
    if (!file) return;
    const bytes = new Uint8Array(await file.arrayBuffer());
    setCapabilityBytes(bytes);
    setCapabilityText(new TextDecoder().decode(bytes));
    setCapabilityFilename(file.name);
  };

  const setCapabilityFromText = (text: string) => {
    setCapabilityText(text);
    setCapabilityBytes(text ? new TextEncoder().encode(text) : null);
    setCapabilityFilename(null);
  };

  const exportBackup = async () => {
    if (passphrase !== confirmPassphrase) {
      setError("Backup passphrases do not match.");
      return;
    }
    const result = await runAction("backup", () => api.exportBackup(profile, passphrase));
    if (!result) return;
    downloadBytes(result.filename, result.encryptedBytes, "application/octet-stream");
    closeBackupDialog();
    setNotice("Encrypted recovery backup downloaded. Store it separately from this computer.");
  };

  const restoreBackup = async () => {
    if (!restoreBytes) {
      setError("Choose an encrypted .age backup first.");
      return;
    }
    const result = await runAction("restore", () =>
      api.restoreBackup(profile, passphrase, restoreBytes),
    );
    if (!result) return;
    closeBackupDialog();
    await loadRounds(profile);
    setNotice(`Restored ${result.roundCount} round(s) into the ${profileLabel(profile)} profile.`);
  };

  const closeBackupDialog = () => {
    setBackupMode(null);
    setPassphrase("");
    setConfirmPassphrase("");
    setRestoreBytes(null);
    setRestoreFilename(null);
  };

  const resetDemo = async () => {
    if (!window.confirm("Remove the local demo database and its demo hotkey from Keychain?")) return;
    const result = await runAction("reset", () => api.resetDemo());
    if (!result) return;
    await loadRounds("demo");
    setNotice("Local demo state was removed. You can start a clean rehearsal now.");
  };

  const completeChoiceCount = workspace
    ? workspace.round.proposals.filter((proposal) => choices[proposal.id] !== undefined).length
    : 0;
  const delegationReady = Boolean(
    workspace &&
      workspace.progress.bundleCount > 0 &&
      workspace.progress.bundleCount === workspace.progress.confirmedBundleCount,
  );
  const voteReady = Boolean(
    workspace &&
      delegationReady &&
      completeChoiceCount === workspace.round.proposals.length &&
      reviewed &&
      (workspace.round.isActive || profile === "demo"),
  );
  const expectedVoteCount = workspace
    ? workspace.progress.bundleCount * workspace.round.proposals.length
    : 0;
  const roundCanPrepare = Boolean(
    workspace &&
      (profile === "demo" ||
        workspace.round.isActive ||
        ["4", "pending", "session_status_pending"].includes(
          workspace.round.status.trim().toLowerCase(),
        )),
  );
  const custodianBirthday = Number(custodianBirthdayHeight);
  const canRunTestCustodian = Boolean(
    workspace &&
      Number.isSafeInteger(custodianBirthday) &&
      custodianBirthday > 0 &&
      custodianBirthday <= workspace.round.snapshotHeight &&
      custodianMnemonic.trim() &&
      lightwalletdUrl.trim() &&
      custodianAcknowledged,
  );

  return (
    <div className={`app-shell profile-${profile}`}>
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark" aria-hidden="true">
            <img src={appIconUrl} alt="" />
          </div>
          <div>
            <strong>Zcash</strong>
            <span>Custody Voter</span>
          </div>
        </div>

        <div className="network-heading">
          <span>Environment</span>
          <span className={`network-dot ${profile}`} />
        </div>
        <div className="profile-switcher" role="tablist" aria-label="Network profile">
          {PROFILES.map((item) => (
            <button
              key={item.id}
              className={profile === item.id ? "active" : ""}
              onClick={() => setProfile(item.id)}
              role="tab"
              aria-selected={profile === item.id}
              disabled={busy === "vote" || busy === "custodian"}
            >
              <span>{item.label}</span>
              <small>{item.eyebrow}</small>
            </button>
          ))}
        </div>

        {profile === "mainnet" && (
          <div className="mainnet-warning">
            <WarningIcon />
            <div>
              <strong>Production network</strong>
              <span>Votes are real and cannot be changed after submission.</span>
            </div>
          </div>
        )}

        <div className="round-list-heading">
          <span>Rounds</span>
          <button
            className="icon-button"
            aria-label="Refresh rounds"
            title="Refresh rounds"
            onClick={() => void loadRounds(profile, selectedRoundId)}
            disabled={Boolean(busy)}
          >
            <RefreshIcon />
          </button>
        </div>
        <div className="round-list">
          {busy === "rounds" && rounds.length === 0 ? (
            <RoundSkeleton />
          ) : rounds.length === 0 ? (
            <div className="empty-rounds">No authenticated rounds are available.</div>
          ) : (
            rounds.map((round) => (
              <button
                key={round.roundId}
                className={`round-row ${selectedRoundId === round.roundId ? "active" : ""}`}
                onClick={() => void selectRound(round.roundId)}
                disabled={busy === "vote" || busy === "custodian"}
              >
                <div>
                  <span className={`status-pin ${round.isActive ? "live" : "closed"}`} />
                  <strong>{round.title}</strong>
                </div>
                <small>
                  {round.statusLabel}
                  {round.stored ? " · On this device" : ""}
                </small>
              </button>
            ))
          )}
        </div>

        <div className="sidebar-footer">
          <button className="sidebar-action" onClick={() => setBackupMode("export")}>
            <DownloadIcon /> Export recovery backup
          </button>
          <button className="sidebar-action" onClick={() => setBackupMode("restore")}>
            <UploadIcon /> Restore recovery backup
          </button>
          {profile === "demo" && (
            <button
              className="sidebar-action danger"
              onClick={() => void resetDemo()}
              disabled={Boolean(busy)}
            >
              <TrashIcon /> Reset local demo
            </button>
          )}
          <div className="security-footnote">
            <LockIcon />
            <span>Hotkeys remain in your operating-system Keychain.</span>
          </div>
        </div>
      </aside>

      <main className="main-content">
        <header className="topbar">
          <div>
            <span className="topbar-eyebrow">Customer voting console</span>
            <h1>{workspace?.round.title ?? "Select a voting round"}</h1>
          </div>
          <div className={`profile-badge ${profile}`}>
            <span className={`network-dot ${profile}`} />
            {profileLabel(profile)}
          </div>
        </header>

        {error && (
          <div className="alert error" role="alert">
            <WarningIcon />
            <span>{error}</span>
            <button onClick={() => setError(null)} aria-label="Dismiss error">×</button>
          </div>
        )}
        {notice && (
          <div className="alert success" role="status">
            <CheckIcon />
            <span>{notice}</span>
            <button onClick={() => setNotice(null)} aria-label="Dismiss message">×</button>
          </div>
        )}

        {!workspace ? (
          <EmptyState loading={busy === "rounds" || busy === "workspace"} />
        ) : (
          <div className="workspace">
            <section className="round-hero">
              <div className="round-hero-copy">
                <div className="round-meta">
                  <span className={`status-chip ${workspace.round.isActive ? "active" : "inactive"}`}>
                    {workspace.round.statusLabel}
                  </span>
                  <span>Snapshot {workspace.round.snapshotHeight.toLocaleString()}</span>
                  <span>{workspace.round.proposals.length} proposal{workspace.round.proposals.length === 1 ? "" : "s"}</span>
                </div>
                <p>{workspace.round.description || "No description was provided for this round."}</p>
                <div className="round-id" title={workspace.round.roundId}>
                  <span>Round</span>
                  <code>{shortId(workspace.round.roundId)}</code>
                  <button
                    onClick={() => void copyText(workspace.round.roundId, setNotice)}
                    aria-label="Copy round identifier"
                    title="Copy round identifier"
                  >
                    <CopyIcon />
                  </button>
                </div>
              </div>
              <div className="deadline-card">
                <small>{profile === "demo" ? "Demo mode" : "Voting closes"}</small>
                <strong>{profile === "demo" ? "Never broadcasts" : formatDate(workspace.round.voteEndTime)}</strong>
                <span>
                  {profile === "demo"
                    ? "Real local proofs, synthetic confirmations"
                    : workspace.round.isActive
                      ? "Round is accepting votes"
                      : "Round is not accepting votes"}
                </span>
              </div>
            </section>

            <WorkflowSteps
              targetReady={workspace.progress.targetReady}
              capabilityReady={workspace.progress.capabilityImported}
              delegationReady={delegationReady}
              voteReady={expectedVoteCount > 0 && workspace.progress.confirmedVoteCount >= expectedVoteCount}
            />

            <section className="workflow-card" id="target">
              <CardNumber number="01" done={workspace.progress.targetReady} />
              <div className="card-body">
                <div className="card-heading">
                  <div>
                    <span className="section-kicker">Customer controlled</span>
                    <h2>Create the voting target</h2>
                    <p>
                      A new voting-only hotkey is generated on this computer. Send the public target to your custody provider; the secret never leaves Keychain.
                    </p>
                  </div>
                  <KeyIcon />
                </div>
                {workspace.targetJson ? (
                  <div className="payload-panel public">
                    <div className="payload-label">
                      <span><ShieldIcon /> Public target · safe to send to the custodian</span>
                      <span>{new TextEncoder().encode(workspace.targetJson).length} bytes</span>
                    </div>
                    <code>{workspace.targetJson}</code>
                    <div className="button-row">
                      <button className="secondary" onClick={() => void copyText(workspace.targetJson!, setNotice)}>
                        <CopyIcon /> Copy target
                      </button>
                      <button
                        className="secondary"
                        onClick={() =>
                          downloadText(
                            `voting-target-${workspace.round.roundId.slice(0, 10)}.json`,
                            workspace.targetJson!,
                          )
                        }
                      >
                        <DownloadIcon /> Download JSON
                      </button>
                      {profile === "demo" && (
                        <button className="accent" onClick={() => void generateDemoPayload()} disabled={Boolean(busy)}>
                          Simulate custodian response <ArrowIcon />
                        </button>
                      )}
                    </div>
                    <div className="backup-callout">
                      <LockIcon />
                      <span>
                        Back up now. The public target cannot recreate your hotkey if this device is lost.
                      </span>
                      <button onClick={() => setBackupMode("export")}>Export backup</button>
                    </div>
                  </div>
                ) : (
                  <div className="action-panel">
                    <div>
                      <strong>
                        {roundCanPrepare
                          ? "No target exists for this round"
                          : "This round no longer accepts setup"}
                      </strong>
                      <span>
                        {roundCanPrepare
                          ? "One independent hotkey is created per network and round."
                          : "New targets are limited to pending and active rounds."}
                      </span>
                    </div>
                    <button
                      className="primary"
                      onClick={() => void generateTarget()}
                      disabled={Boolean(busy) || !roundCanPrepare}
                    >
                      {busy === "target" ? <Spinner /> : <KeyIcon />} Generate customer target
                    </button>
                  </div>
                )}
              </div>
            </section>

            <section className={`workflow-card ${!workspace.targetJson ? "locked" : ""}`} id="capability">
              <CardNumber number="02" done={workspace.progress.capabilityImported} />
              <div className="card-body">
                <div className="card-heading">
                  <div>
                    <span className="section-kicker">Custodian handoff</span>
                    <h2>Import the custody payload</h2>
                    <p>
                      Import the exact JSON file returned by your custodian. The app checks its canonical bytes, network, round, hotkey target, bundle order, and transaction hashes before storing anything.
                    </p>
                  </div>
                  <InboxIcon />
                </div>
                {workspace.progress.capabilityImported ? (
                  <div className="verified-panel">
                    <CheckIcon />
                    <div>
                      <strong>Custody payload verified</strong>
                      <span>{workspace.progress.bundleCount} delegation bundle{workspace.progress.bundleCount === 1 ? "" : "s"} · {workspace.progress.delegatedBallots.toLocaleString()} ballots</span>
                      {workspace.capabilityDigest && <code>SHA-256 {workspace.capabilityDigest}</code>}
                    </div>
                  </div>
                ) : (
                  <>
                    {profile === "testnet" && (
                      <div className={`test-custodian ${custodianOpen ? "open" : ""}`}>
                        <div className="test-custodian-heading">
                          <div className="test-custodian-icon"><WalletIcon /></div>
                          <div>
                            <span className="section-kicker">Local integration harness</span>
                            <strong>Generate this payload with a real Testnet wallet</strong>
                            <p>
                              Temporarily act as the custodian so you can test the exact customer handoff end to end.
                            </p>
                          </div>
                          <button
                            className="secondary"
                            onClick={() => setCustodianOpen((current) => !current)}
                            disabled={busy === "custodian"}
                          >
                            {custodianOpen ? "Hide setup" : "Open test custodian"}
                          </button>
                        </div>

                        {custodianOpen && (
                          <div className="test-custodian-body">
                            <div className="testnet-safety-note">
                              <WarningIcon />
                              <span>
                                Testnet only. Use a disposable wallet. This creates real delegation proofs and broadcasts real transactions to the Stage vote chain, but it does not move or spend ZEC.
                              </span>
                            </div>

                            <div className="custodian-field-grid">
                              <label className="field-label">
                                Wallet birthday height
                                <input
                                  type="number"
                                  min={280000}
                                  max={workspace.round.snapshotHeight}
                                  step={1}
                                  value={custodianBirthdayHeight}
                                  onChange={(event) => {
                                    setCustodianBirthdayHeight(event.target.value);
                                    setCustodianResult(null);
                                  }}
                                  placeholder={`At or before ${workspace.round.snapshotHeight}`}
                                  disabled={busy === "custodian"}
                                />
                                <small>
                                  First block the wallet should scan. Use a height before its earliest transaction. ZIP 32 account 0 is used automatically.
                                </small>
                              </label>

                              <label className="field-label">
                                Testnet lightwalletd
                                <input
                                  type="url"
                                  value={lightwalletdUrl}
                                  onChange={(event) => setLightwalletdUrl(event.target.value)}
                                  spellCheck={false}
                                  disabled={busy === "custodian"}
                                />
                              </label>
                            </div>

                            <label className="field-label mnemonic-field">
                              Testnet wallet mnemonic
                              <span className="secret-input">
                                <input
                                  type={showCustodianMnemonic ? "text" : "password"}
                                  value={custodianMnemonic}
                                  onChange={(event) => setCustodianMnemonic(event.target.value)}
                                  placeholder="12, 15, 18, 21, or 24 words"
                                  autoComplete="off"
                                  spellCheck={false}
                                  disabled={busy === "custodian"}
                                />
                                <button
                                  type="button"
                                  onClick={() => setShowCustodianMnemonic((current) => !current)}
                                  aria-label={showCustodianMnemonic ? "Hide mnemonic" : "Show mnemonic"}
                                >
                                  {showCustodianMnemonic ? "Hide" : "Show"}
                                </button>
                              </span>
                              <small>
                                Used to recover and sign with account 0, then cleared. It is never written to disk or included in the customer payload.
                              </small>
                            </label>

                            <label className="custodian-acknowledgement">
                              <input
                                type="checkbox"
                                checked={custodianAcknowledged}
                                onChange={(event) => setCustodianAcknowledged(event.target.checked)}
                                disabled={busy === "custodian"}
                              />
                              <span><CheckIcon /></span>
                              I am using a disposable Testnet wallet and understand this will broadcast to the Stage vote chain.
                            </label>

                            {busy === "custodian" && custodianProgress && (
                              <div className="custodian-progress">
                                <div>
                                  <Spinner />
                                  <span>
                                    <strong>{custodianProgress.message}</strong>
                                    {custodianProgress.bundleIndex !== null && (
                                      <small>
                                        Bundle {custodianProgress.bundleIndex + 1}
                                        {custodianProgress.bundleCount !== null ? ` of ${custodianProgress.bundleCount}` : ""}
                                      </small>
                                    )}
                                  </span>
                                </div>
                                <div className={`progress-track ${custodianProgress.progress === null ? "indeterminate" : ""}`}>
                                  <span style={custodianProgress.progress === null ? undefined : { width: `${custodianProgress.progress * 100}%` }} />
                                </div>
                                <p>Keep the app open. The first proof can take several minutes.</p>
                              </div>
                            )}

                            <div className="button-row custodian-actions">
                              <span>
                                The app creates an isolated wallet, syncs it to the round snapshot, and removes it after signing.
                              </span>
                              <button
                                className="primary"
                                onClick={() => void generateTestnetPayload()}
                                disabled={!canRunTestCustodian || Boolean(busy)}
                              >
                                {busy === "custodian" ? <Spinner /> : <ShieldIcon />}
                                Recover, build, and broadcast
                              </button>
                            </div>

                            {custodianResult && (
                              <div className="custodian-result">
                                <CheckIcon />
                                <span>
                                  <strong>Exact custody payload generated</strong>
                                  <small>{custodianResult.bundleCount} bundle{custodianResult.bundleCount === 1 ? "" : "s"} · SHA-256 {shortId(custodianResult.digest)}</small>
                                </span>
                                <button
                                  className="secondary"
                                  onClick={() => downloadText(capabilityFilename ?? "testnet-custody-capability.json", custodianResult.capabilityJson)}
                                >
                                  <DownloadIcon /> Download
                                </button>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    )}

                    {profile === "testnet" && <div className="handoff-divider"><span>or import the provider handoff</span></div>}
                    <label className="file-drop">
                      <UploadIcon />
                      <strong>{capabilityFilename ?? "Choose the custodian JSON file"}</strong>
                      <span>or paste the exact compact JSON below</span>
                      <input
                        type="file"
                        accept="application/json,.json"
                        onChange={(event) => void handleCapabilityFile(event.target.files?.[0])}
                        disabled={!workspace.targetJson || Boolean(busy)}
                      />
                    </label>
                    <textarea
                      className="payload-input"
                      value={capabilityText}
                      onChange={(event) => setCapabilityFromText(event.target.value)}
                      placeholder='{"format_version":1,"vote_chain_id":"…"}'
                      spellCheck={false}
                      disabled={!workspace.targetJson || Boolean(busy)}
                    />
                    <div className="button-row align-end">
                      <span className="byte-count">{capabilityBytes?.length ?? 0} exact bytes</span>
                      <button
                        className="primary"
                        onClick={() => void importCapability()}
                        disabled={!workspace.targetJson || !capabilityBytes || Boolean(busy)}
                      >
                        {busy === "capability" ? <Spinner /> : <ShieldIcon />} Verify and import
                      </button>
                    </div>
                  </>
                )}
              </div>
            </section>

            <section className={`workflow-card ${!workspace.progress.capabilityImported ? "locked" : ""}`} id="confirmation">
              <CardNumber number="03" done={delegationReady} />
              <div className="card-body">
                <div className="card-heading">
                  <div>
                    <span className="section-kicker">On-chain authorization</span>
                    <h2>Confirm every delegation</h2>
                    <p>
                      Voting stays blocked until every transaction named in the custody payload is confirmed and its public VAN position is recorded.
                    </p>
                  </div>
                  <ChainIcon />
                </div>
                <div className="confirmation-panel">
                  <div className="confirmation-meter">
                    <div>
                      <strong>{workspace.progress.confirmedBundleCount}</strong>
                      <span>of {workspace.progress.bundleCount || "—"} bundles confirmed</span>
                    </div>
                    <div className="meter-track">
                      <span
                        style={{
                          width: `${workspace.progress.bundleCount ? (workspace.progress.confirmedBundleCount / workspace.progress.bundleCount) * 100 : 0}%`,
                        }}
                      />
                    </div>
                  </div>
                  <button
                    className={delegationReady ? "secondary" : "primary"}
                    onClick={() => void checkConfirmations()}
                    disabled={!workspace.progress.capabilityImported || Boolean(busy)}
                  >
                    {busy === "confirmation" ? <Spinner /> : <RefreshIcon />}
                    {profile === "demo" && !delegationReady
                      ? "Simulate confirmations"
                      : delegationReady
                        ? "Check again"
                        : "Check confirmation"}
                  </button>
                </div>
              </div>
            </section>

            <section className={`workflow-card vote-card ${!delegationReady ? "locked" : ""}`} id="vote">
              <CardNumber
                number="04"
                done={expectedVoteCount > 0 && workspace.progress.confirmedVoteCount >= expectedVoteCount}
              />
              <div className="card-body">
                <div className="card-heading">
                  <div>
                    <span className="section-kicker">Private ballot</span>
                    <h2>Review and cast your vote</h2>
                    <p>
                      Your selections are proved and signed locally. One vote is produced for each delegated bundle and proposal.
                    </p>
                  </div>
                  <BallotIcon />
                </div>

                <div className="proposal-list">
                  {workspace.round.proposals.map((proposal, proposalIndex) => (
                    <fieldset
                      className="proposal"
                      key={proposal.id}
                      disabled={
                        !delegationReady ||
                        busy === "vote" ||
                        workspace.votes.some((vote) => vote.proposalId === proposal.id)
                      }
                    >
                      <legend>
                        <span>{String(proposalIndex + 1).padStart(2, "0")}</span>
                        <div>
                          <strong>{proposal.title}</strong>
                          {proposal.description && <small>{proposal.description}</small>}
                          {workspace.votes.some((vote) => vote.proposalId === proposal.id) && (
                            <small>Selection locked by persisted vote recovery state.</small>
                          )}
                        </div>
                      </legend>
                      <div className="option-grid">
                        {proposal.options.map((option) => {
                          const checked = choices[proposal.id] === option.index;
                          return (
                            <label className={`vote-option ${checked ? "selected" : ""}`} key={option.index}>
                              <input
                                type="radio"
                                name={`proposal-${proposal.id}`}
                                checked={checked}
                                onChange={() => {
                                  setChoices((current) => ({ ...current, [proposal.id]: option.index }));
                                  setReviewed(false);
                                }}
                              />
                              <span className="radio-mark"><span /></span>
                              <span>
                                <strong>{option.label}</strong>
                                {option.description && <small>{option.description}</small>}
                              </span>
                            </label>
                          );
                        })}
                      </div>
                    </fieldset>
                  ))}
                </div>

                <div className="review-panel">
                  <div className="review-stats">
                    <div><span>Selections</span><strong>{completeChoiceCount}/{workspace.round.proposals.length}</strong></div>
                    <div><span>Delegated bundles</span><strong>{workspace.progress.bundleCount || "—"}</strong></div>
                    <div><span>Proof-backed votes</span><strong>{expectedVoteCount || "—"}</strong></div>
                  </div>
                  <label className="review-check">
                    <input
                      type="checkbox"
                      checked={reviewed}
                      onChange={(event) => setReviewed(event.target.checked)}
                      disabled={!delegationReady || completeChoiceCount !== workspace.round.proposals.length || busy === "vote"}
                    />
                    <span><CheckIcon /></span>
                    I reviewed the network, round, and every selection above.
                  </label>
                  {busy === "vote" && voteProgress && (
                    <div className="vote-progress">
                      <div>
                        <Spinner />
                        <span>
                          <strong>{voteProgress.message}</strong>
                          {(voteProgress.bundleIndex !== null || voteProgress.proposalId !== null) && (
                            <small>
                              {voteProgress.bundleIndex !== null ? `Bundle ${voteProgress.bundleIndex + 1}` : ""}
                              {voteProgress.bundleIndex !== null && voteProgress.proposalId !== null ? " · " : ""}
                              {voteProgress.proposalId !== null ? `Proposal ${voteProgress.proposalId}` : ""}
                            </small>
                          )}
                        </span>
                      </div>
                      <div className={`progress-track ${voteProgress.progress === null ? "indeterminate" : ""}`}>
                        <span style={voteProgress.progress === null ? undefined : { width: `${voteProgress.progress * 100}%` }} />
                      </div>
                      <p>Keep this app open. Proof generation can take several minutes on the first run.</p>
                    </div>
                  )}
                  <button className="cast-button" onClick={() => void castVotes()} disabled={!voteReady || Boolean(busy)}>
                    {busy === "vote" ? <Spinner /> : <ShieldIcon />}
                    {profile === "demo" ? "Run local proof rehearsal" : "Generate proofs and cast vote"}
                    {busy !== "vote" && <ArrowIcon />}
                  </button>
                  {!workspace.round.isActive && profile !== "demo" && (
                    <p className="closed-note">This authenticated round is {workspace.round.statusLabel.toLowerCase()} and cannot accept a new vote.</p>
                  )}
                </div>

                {voteResult && (
                  <div className="result-panel">
                    <div className="result-icon"><CheckIcon /></div>
                    <div>
                      <strong>{voteResult.demo ? "Local rehearsal complete" : "Vote confirmed"}</strong>
                      <p>
                        {voteResult.proofCount} proof-backed vote{voteResult.proofCount === 1 ? "" : "s"} processed across {workspace.progress.bundleCount} bundle{workspace.progress.bundleCount === 1 ? "" : "s"}.
                      </p>
                      <div className="transaction-list">
                        {voteResult.transactions.map((transaction) => (
                          <code key={`${transaction.bundleIndex}-${transaction.proposalId}`}>
                            B{transaction.bundleIndex + 1} · P{transaction.proposalId} · {shortId(transaction.txHash)}
                          </code>
                        ))}
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </section>
          </div>
        )}
      </main>

      {backupMode && (
        <div className="modal-backdrop" role="presentation" onMouseDown={closeBackupDialog}>
          <section className="modal" role="dialog" aria-modal="true" onMouseDown={(event) => event.stopPropagation()}>
            <button className="modal-close" onClick={closeBackupDialog} aria-label="Close">×</button>
            <div className="modal-icon"><LockIcon /></div>
            <span className="section-kicker">{profileLabel(profile)} profile</span>
            <h2>{backupMode === "export" ? "Export recovery backup" : "Restore recovery backup"}</h2>
            <p>
              {backupMode === "export"
                ? "The encrypted file contains this profile’s voting database and hotkeys. It never contains custody funds or wallet seed material."
                : "Restoring replaces this profile’s local voting state after the encrypted file and every hotkey-to-target binding are verified."}
            </p>
            {backupMode === "restore" && (
              <label className="file-drop compact">
                <UploadIcon />
                <strong>{restoreFilename ?? "Choose a Zcash Custody Voter .age backup"}</strong>
                <input
                  type="file"
                  accept=".age,application/octet-stream"
                  onChange={(event) => {
                    const file = event.target.files?.[0];
                    if (!file) return;
                    void file.arrayBuffer().then((buffer) => {
                      setRestoreBytes(new Uint8Array(buffer));
                      setRestoreFilename(file.name);
                    });
                  }}
                />
              </label>
            )}
            <label className="field-label">
              Backup passphrase
              <input
                type="password"
                value={passphrase}
                onChange={(event) => setPassphrase(event.target.value)}
                autoComplete="new-password"
                placeholder="At least 12 characters"
              />
            </label>
            {backupMode === "export" && (
              <label className="field-label">
                Confirm passphrase
                <input
                  type="password"
                  value={confirmPassphrase}
                  onChange={(event) => setConfirmPassphrase(event.target.value)}
                  autoComplete="new-password"
                />
              </label>
            )}
            <div className="modal-warning">
              <WarningIcon />
              <span>There is no passphrase recovery. Keep the passphrase separately from the backup file.</span>
            </div>
            <button
              className="primary modal-primary"
              disabled={passphrase.length < 12 || Boolean(busy) || (backupMode === "restore" && !restoreBytes)}
              onClick={() => void (backupMode === "export" ? exportBackup() : restoreBackup())}
            >
              {busy === "backup" || busy === "restore" ? <Spinner /> : backupMode === "export" ? <DownloadIcon /> : <UploadIcon />}
              {backupMode === "export" ? "Encrypt and download" : "Verify and restore"}
            </button>
          </section>
        </div>
      )}
    </div>
  );
}

function WorkflowSteps({
  targetReady,
  capabilityReady,
  delegationReady,
  voteReady,
}: {
  targetReady: boolean;
  capabilityReady: boolean;
  delegationReady: boolean;
  voteReady: boolean;
}) {
  const steps = [
    ["Target", targetReady],
    ["Payload", capabilityReady],
    ["Confirmed", delegationReady],
    ["Voted", voteReady],
  ] as const;
  return (
    <nav className="workflow-steps" aria-label="Voting progress">
      {steps.map(([label, done], index) => (
        <div className={done ? "done" : ""} key={label}>
          <span>{done ? <CheckIcon /> : index + 1}</span>
          <strong>{label}</strong>
          {index < steps.length - 1 && <i />}
        </div>
      ))}
    </nav>
  );
}

function CardNumber({ number, done }: { number: string; done: boolean }) {
  return <div className={`card-number ${done ? "done" : ""}`}>{done ? <CheckIcon /> : number}</div>;
}

function EmptyState({ loading }: { loading: boolean }) {
  return (
    <div className="empty-state">
      {loading ? <Spinner /> : <BallotIcon />}
      <h2>{loading ? "Loading authenticated rounds" : "No round selected"}</h2>
      <p>{loading ? "Checking the pinned voting configuration and vote chain." : "Choose an authenticated round from the sidebar to begin."}</p>
    </div>
  );
}

function RoundSkeleton() {
  return (
    <div className="round-skeleton">
      <span />
      <span />
      <span />
    </div>
  );
}

function existingChoices(workspace: RoundWorkspace): Record<number, number> {
  const result: Record<number, number> = {};
  for (const vote of workspace.votes) {
    if (result[vote.proposalId] === undefined) result[vote.proposalId] = vote.choice;
  }
  return result;
}

function profileLabel(profile: Profile) {
  return PROFILES.find((item) => item.id === profile)?.label ?? profile;
}

function formatDate(timestamp: number) {
  if (!timestamp) return "Not provided";
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(timestamp * 1000));
}

function shortId(value: string) {
  return value.length > 22 ? `${value.slice(0, 12)}…${value.slice(-8)}` : value;
}

async function copyText(value: string, notify: (message: string) => void) {
  await navigator.clipboard.writeText(value);
  notify("Copied to clipboard.");
}

function downloadText(filename: string, value: string) {
  downloadBytes(filename, Array.from(new TextEncoder().encode(value)), "application/json");
}

function downloadBytes(filename: string, bytes: number[], type: string) {
  const blob = new Blob([new Uint8Array(bytes)], { type });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  anchor.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 0);
}

function Icon({ children }: { children: React.ReactNode }) {
  return <svg viewBox="0 0 24 24" aria-hidden="true" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">{children}</svg>;
}

const CheckIcon = () => <Icon><path d="m5 12 4 4L19 6" /></Icon>;
const WarningIcon = () => <Icon><path d="M12 3 2.7 20h18.6L12 3Z" /><path d="M12 9v4" /><path d="M12 17h.01" /></Icon>;
const RefreshIcon = () => <Icon><path d="M20 11a8 8 0 0 0-14.8-4L3 10" /><path d="M3 4v6h6" /><path d="M4 13a8 8 0 0 0 14.8 4L21 14" /><path d="M21 20v-6h-6" /></Icon>;
const DownloadIcon = () => <Icon><path d="M12 3v12" /><path d="m7 10 5 5 5-5" /><path d="M4 20h16" /></Icon>;
const UploadIcon = () => <Icon><path d="M12 16V4" /><path d="m7 9 5-5 5 5" /><path d="M4 20h16" /></Icon>;
const TrashIcon = () => <Icon><path d="M4 7h16" /><path d="m9 7 1-3h4l1 3" /><path d="m6 7 1 14h10l1-14" /></Icon>;
const LockIcon = () => <Icon><rect x="5" y="10" width="14" height="11" rx="2" /><path d="M8 10V7a4 4 0 0 1 8 0v3" /></Icon>;
const CopyIcon = () => <Icon><rect x="8" y="8" width="11" height="12" rx="2" /><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h2" /></Icon>;
const KeyIcon = () => <Icon><circle cx="8" cy="15" r="4" /><path d="m11 12 9-9" /><path d="m17 6 2 2" /><path d="m14 9 2 2" /></Icon>;
const ShieldIcon = () => <Icon><path d="M12 3 5 6v5c0 4.6 2.8 8.1 7 10 4.2-1.9 7-5.4 7-10V6l-7-3Z" /><path d="m9 12 2 2 4-5" /></Icon>;
const InboxIcon = () => <Icon><path d="M4 4h16v16H4z" /><path d="m4 13 4-4h8l4 4" /><path d="M8 13h8" /></Icon>;
const ChainIcon = () => <Icon><path d="m9 15-2 2a3 3 0 1 1-4-4l3-3a3 3 0 0 1 4 0" /><path d="m15 9 2-2a3 3 0 1 1 4 4l-3 3a3 3 0 0 1-4 0" /><path d="m8 16 8-8" /></Icon>;
const BallotIcon = () => <Icon><path d="M6 3h12v18H6z" /><path d="M9 7h6" /><path d="M9 11h6" /><path d="M9 15h3" /></Icon>;
const ArrowIcon = () => <Icon><path d="M5 12h14" /><path d="m14 7 5 5-5 5" /></Icon>;
const WalletIcon = () => <Icon><path d="M4 7.5h15a2 2 0 0 1 2 2v9.5H4a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h13" /><path d="M17 12h4" /><circle cx="17" cy="14" r=".5" fill="currentColor" stroke="none" /></Icon>;

function Spinner() {
  return <span className="spinner" aria-label="Loading" />;
}

export default App;
