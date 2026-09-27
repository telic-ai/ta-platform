import { useMemo, useState } from "react";
import { CompanyClient } from "@ta-platform/api-client";
import { ClientContext } from "./api";
import { Layout } from "./components/Layout";
import { SignIn } from "./components/SignIn";
import { Dashboard } from "./pages/Dashboard";
import { Interviews } from "./pages/Interviews";
import { LiveView } from "./pages/LiveView";
import { Replay } from "./pages/Replay";
import { Settings } from "./pages/Settings";
import { useRoute } from "./route";
import { sessionStore } from "./session";

export interface AppProps {
  /** Admin API base URL. */
  adminUrl: string;
  /** Live-monitor base URL. */
  liveUrl: string;
  /** Candidate app URL, for invite links. */
  candidateUrl: string;
  fetch?: typeof fetch;
  /** Replay autoplay step, in ms. */
  playStepMs?: number;
}

export function App({ adminUrl, liveUrl, candidateUrl, fetch, playStepMs }: AppProps) {
  const [token, setToken] = useState(sessionStore.load);
  const route = useRoute();
  const client = useMemo(
    () => (token ? new CompanyClient({ baseUrl: adminUrl, liveUrl, token, fetch }) : null),
    [adminUrl, liveUrl, token, fetch],
  );

  if (!client) {
    return (
      <SignIn
        adminUrl={adminUrl}
        fetch={fetch}
        onSignedIn={(next) => {
          sessionStore.save(next);
          setToken(next);
        }}
      />
    );
  }
  const signOut = () => {
    sessionStore.clear();
    setToken(undefined);
  };
  return (
    <ClientContext.Provider value={client}>
      <Layout route={route} onSignOut={signOut}>
        {route.page === "dashboard" && <Dashboard />}
        {route.page === "interviews" && <Interviews candidateUrl={candidateUrl} />}
        {route.page === "live" && <LiveView key={route.interviewId} interviewId={route.interviewId} />}
        {route.page === "replay" && <Replay key={route.interviewId} interviewId={route.interviewId} playStepMs={playStepMs} />}
        {route.page === "settings" && <Settings />}
      </Layout>
    </ClientContext.Provider>
  );
}
