import type { WORKSPACE_STARTERS } from "@ta-platform/api-client";

/** Returns the first fenced code block in a Markdown answer, if any. */
export function firstCodeBlock(markdown: string): string | undefined {
  const match = /```[^\n]*\n([\s\S]*?)```/.exec(markdown);
  return match?.[1];
}

/** Starter files are shared with the company app, whose replay rebuilds from them. */
export { WORKSPACE_STARTERS as LANGUAGES } from "@ta-platform/api-client";

export type Language = keyof typeof WORKSPACE_STARTERS;
