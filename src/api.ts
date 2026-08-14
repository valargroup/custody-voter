import { invoke } from "@tauri-apps/api/core";
import type {
  BackupResult,
  CastVotesResult,
  ImportResult,
  Profile,
  RestoreResult,
  ResetResult,
  RoundCard,
  RoundWorkspace,
  TargetResult,
  TestCustodianResult,
  VoteChoice,
} from "./types";

export const api = {
  listRounds(profile: Profile) {
    return invoke<RoundCard[]>("list_rounds", { profile });
  },
  getWorkspace(profile: Profile, roundId: string) {
    return invoke<RoundWorkspace>("get_round_workspace", { profile, roundId });
  },
  generateTarget(profile: Profile, roundId: string) {
    return invoke<TargetResult>("generate_target", { profile, roundId });
  },
  generateDemoCapability(roundId: string) {
    return invoke<string>("generate_demo_capability", { roundId });
  },
  importCapability(profile: Profile, roundId: string, bytes: Uint8Array) {
    return invoke<ImportResult>("import_capability", {
      profile,
      roundId,
      capabilityBytes: Array.from(bytes),
    });
  },
  checkDelegations(profile: Profile, roundId: string) {
    return invoke<ImportResult>("check_delegation_confirmations", {
      profile,
      roundId,
    });
  },
  generateTestnetCustodyPayload(
    roundId: string,
    birthdayHeight: number,
    mnemonic: string,
    lightwalletdUrl: string,
  ) {
    return invoke<TestCustodianResult>("generate_testnet_custody_payload", {
      roundId,
      birthdayHeight,
      mnemonic,
      lightwalletdUrl,
    });
  },
  castVotes(profile: Profile, roundId: string, choices: VoteChoice[]) {
    return invoke<CastVotesResult>("cast_votes", {
      profile,
      roundId,
      choices,
    });
  },
  exportBackup(profile: Profile, passphrase: string) {
    return invoke<BackupResult>("export_backup", { profile, passphrase });
  },
  restoreBackup(profile: Profile, passphrase: string, bytes: Uint8Array) {
    return invoke<RestoreResult>("restore_backup", {
      profile,
      passphrase,
      encryptedBytes: Array.from(bytes),
    });
  },
  prepareReset() {
    return invoke<string>("prepare_reset");
  },
  resetAllData(confirmationToken: string) {
    return invoke<ResetResult>("reset_all_data", { confirmationToken });
  },
};

export function errorMessage(error: unknown): string {
  if (typeof error === "string") return error;
  if (error instanceof Error) return error.message;
  return "Something went wrong. Please try again.";
}
