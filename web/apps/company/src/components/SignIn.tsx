import { useState } from "react";
import { CompanyClient } from "@ta-platform/api-client";
import { errorMessage } from "../api";

interface Props {
  adminUrl: string;
  fetch?: typeof fetch;
  onSignedIn: (token: string) => void;
}

/**
 * Member sign-in. Until the auth service issues member sessions, members
 * paste a session token (see cmd/demo-seed); it is checked against the
 * Admin API before it is kept.
 */
export function SignIn({ adminUrl, fetch, onSignedIn }: Props) {
  const [token, setToken] = useState("");
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError(undefined);
    try {
      await new CompanyClient({ baseUrl: adminUrl, token: token.trim(), fetch }).getCompany();
      onSignedIn(token.trim());
    } catch (e) {
      setError((e as { status?: number }).status === 401 ? "That session token is not valid." : errorMessage(e));
    } finally {
      setBusy(false);
    }
  }

  return (
    <main className="signin">
      <form onSubmit={submit} className="card">
        <h1>Lucid AI</h1>
        <p className="muted">Sign in to your company workspace.</p>
        <label>
          Session token
          <input value={token} onChange={(e) => setToken(e.target.value)} autoComplete="off" spellCheck={false} required />
        </label>
        {error && <p role="alert" className="error">{error}</p>}
        <button type="submit" disabled={busy || !token.trim()}>
          {busy ? "Signing in…" : "Sign in"}
        </button>
      </form>
    </main>
  );
}
