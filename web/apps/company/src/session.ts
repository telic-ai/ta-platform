const KEY = "ta-platform.company.session";

/** Keeps the member's session token for this tab only. */
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
