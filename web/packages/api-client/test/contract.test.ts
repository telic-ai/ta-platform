import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { makePatch } from "../src/diffs";

// Shared with internal/candidateworkspace/unidiff_contract_test.go, which
// checks the workspace accepts every patch with these line counts.
interface Fixture {
  name: string;
  path: string;
  before: string;
  after: string;
  patch: string;
  linesAdded: number;
  linesRemoved: number;
}

const fixtures: Fixture[] = JSON.parse(readFileSync(new URL("./fixtures/patches.json", import.meta.url), "utf8"));

describe("patch contract with the workspace", () => {
  it.each(fixtures)("$name", (fixture) => {
    expect(makePatch(fixture.path, fixture.before, fixture.after)).toBe(fixture.patch);
  });
});
