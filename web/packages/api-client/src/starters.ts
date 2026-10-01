/**
 * Files a Candidate Workspace starts with, per language. The candidate
 * app's diffs are relative to these, so replay rebuilds files from them.
 */
export const WORKSPACE_STARTERS = {
  python: { label: "Python", file: "main.py", starter: "def solve():\n    pass\n\n\nprint(solve())\n" },
  javascript: { label: "JavaScript", file: "main.js", starter: "function solve() {}\n\nconsole.log(solve());\n" },
} as const;

/** The starting content of every workspace file, by path. */
export function starterFiles(): Record<string, string> {
  return Object.fromEntries(Object.values(WORKSPACE_STARTERS).map((s) => [s.file, s.starter]));
}
