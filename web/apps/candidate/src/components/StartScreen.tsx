import { useState, type FormEvent } from "react";
import { WorkspaceError, type WorkspaceClient } from "@ta-platform/api-client";

interface Props {
  client: WorkspaceClient;
  initialInvite?: string;
  onStarted: (token: string) => void;
}

export function StartScreen({ client, initialInvite = "", onStarted }: Props) {
  const [invite, setInvite] = useState(initialInvite);
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);

  async function start(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError(undefined);
    try {
      const session = await client.startSession(invite.trim());
      onStarted(session.accessToken);
    } catch (err) {
      setError(err instanceof WorkspaceError && err.status === 401 ? "This invite is invalid, expired, or already used." : "Could not start the session. Try again.");
    } finally {
      setBusy(false);
    }
  }

  return (
    <form className="start" onSubmit={start}>
      <h1>Start your interview</h1>
      <label>
        Invite code
        <input value={invite} onChange={(e) => setInvite(e.target.value)} autoComplete="off" required />
      </label>
      <button type="submit" disabled={busy || invite.trim() === ""}>
        {busy ? "Starting…" : "Start"}
      </button>
      {error && <p role="alert">{error}</p>}
    </form>
  );
}
