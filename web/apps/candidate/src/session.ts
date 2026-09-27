const KEY = "ta-platform.candidate.session";

/**
 * Keeps the session token for this tab only, so a reload does not strand a
 * candidate whose invite was already used.
 */
export const sessionStore = {
  load(): string | undefined {
    try {
      return sessionStorage.getItem(KEY) ?? undefined;
    } catch {
      return undefined;
    }
  },
  save(token: string): void {
    try {
      sessionStorage.setItem(KEY, token);
    } catch {
      // Storage unavailable: the session lasts until reload.
    }
  },
  clear(): void {
    try {
      sessionStorage.removeItem(KEY);
    } catch {
      // Nothing stored.
    }
  },
};
