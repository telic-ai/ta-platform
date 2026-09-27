import { useMemo, useState } from "react";
import { WorkspaceClient } from "@ta-platform/api-client";
import { StartScreen } from "./components/StartScreen";
import { Workspace } from "./components/Workspace";
import { sessionStore } from "./session";

interface Props {
  /** The Candidate Workspace URL: the only service this app calls. */
  workspaceUrl: string;
  fetch?: typeof fetch;
  invite?: string;
  diffDelayMs?: number;
}

export function App({ workspaceUrl, fetch, invite, diffDelayMs }: Props) {
  const [token, setToken] = useState(sessionStore.load);
  const client = useMemo(() => new WorkspaceClient({ baseUrl: workspaceUrl, fetch, token }), [workspaceUrl, fetch, token]);

  if (!token) {
    return (
      <StartScreen
        client={client}
        initialInvite={invite}
        onStarted={(next) => {
          sessionStore.save(next);
          setToken(next);
        }}
      />
    );
  }
  return <Workspace client={client} diffDelayMs={diffDelayMs} />;
}
