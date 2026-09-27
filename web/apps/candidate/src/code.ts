/** Returns the first fenced code block in a Markdown answer, if any. */
export function firstCodeBlock(markdown: string): string | undefined {
  const match = /```[^\n]*\n([\s\S]*?)```/.exec(markdown);
  return match?.[1];
}

export const LANGUAGES = {
  python: { label: "Python", file: "main.py", starter: "def solve():\n    pass\n\n\nprint(solve())\n" },
  javascript: { label: "JavaScript", file: "main.js", starter: "function solve() {}\n\nconsole.log(solve());\n" },
} as const;

export type Language = keyof typeof LANGUAGES;
