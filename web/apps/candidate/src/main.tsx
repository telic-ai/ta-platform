import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./styles.css";

const invite = new URLSearchParams(window.location.search).get("invite") ?? undefined;
if (invite) {
  // Keep the one-time invite out of history and referrers.
  window.history.replaceState(null, "", window.location.pathname);
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App workspaceUrl={import.meta.env.VITE_WORKSPACE_URL ?? window.location.origin} invite={invite} />
  </StrictMode>,
);
