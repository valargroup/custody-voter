export type Profile = "mainnet" | "testnet" | "demo";

export interface AppOption {
  index: number;
  label: string;
  description: string;
}

export interface AppProposal {
  id: number;
  title: string;
  description: string;
  options: AppOption[];
}

export interface ServiceEndpoint {
  url: string;
  label: string;
}

export interface VotingRoundParams {
  vote_round_id: string;
  snapshot_height: number;
  ea_pk: number[];
  nc_root: number[];
  nullifier_imt_root: number[];
}

export interface RoundSnapshot {
  profile: Profile;
  chainId: string;
  roundId: string;
  title: string;
  description: string;
  status: string;
  statusLabel: string;
  isActive: boolean;
  snapshotHeight: number;
  voteEndTime: number;
  ceremonyStartTime: number | null;
  proposals: AppProposal[];
  params: VotingRoundParams;
  voteServers: ServiceEndpoint[];
  authenticated: boolean;
}

export interface RoundProgress {
  targetReady: boolean;
  capabilityImported: boolean;
  bundleCount: number;
  confirmedBundleCount: number;
  delegatedBallots: number;
  voteCount: number;
  submittedVoteCount: number;
  confirmedVoteCount: number;
}

export interface RoundCard extends RoundSnapshot {
  progress: RoundProgress;
  stored: boolean;
}

export interface VoteRecord {
  bundleIndex: number;
  proposalId: number;
  choice: number;
  txHash: string | null;
  vcTreePosition: number | null;
}

export interface RoundWorkspace {
  round: RoundSnapshot;
  progress: RoundProgress;
  targetJson: string | null;
  capabilityDigest: string | null;
  votes: VoteRecord[];
}

export interface TargetResult {
  targetJson: string;
  reused: boolean;
}

export interface ImportResult {
  digest: string;
  bundleCount: number;
  confirmedBundleCount: number;
  pendingTransactionHashes: string[];
}

export interface TestCustodianResult {
  capabilityJson: string;
  digest: string;
  bundleCount: number;
  submittedBundleCount: number;
  delegationTransactionHashes: string[];
}

export interface TestCustodianProgressEvent {
  roundId: string;
  phase: string;
  bundleIndex: number | null;
  bundleCount: number | null;
  progress: number | null;
  message: string;
}

export interface VoteChoice {
  proposalId: number;
  choice: number;
}

export interface VoteTransactionResult {
  bundleIndex: number;
  proposalId: number;
  txHash: string;
  vcTreePosition: number;
  sharesSubmitted: number;
}

export interface CastVotesResult {
  demo: boolean;
  proofCount: number;
  transactions: VoteTransactionResult[];
}

export interface VoteProgressEvent {
  roundId: string;
  phase: string;
  bundleIndex: number | null;
  proposalId: number | null;
  progress: number | null;
  message: string;
}

export interface BackupResult {
  filename: string;
  encryptedBytes: number[];
}

export interface RestoreResult {
  profile: Profile;
  roundCount: number;
}
