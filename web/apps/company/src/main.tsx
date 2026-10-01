import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import "./styles.css";

const origin = window.location.origin;
createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App
      adminUrl={import.meta.env.VITE_ADMIN_API_URL ?? `${origin}/api`}
      liveUrl={import.meta.env.VITE_LIVE_URL ?? `${origin}/live-api`}
      candidateUrl={import.meta.env.VITE_CANDIDATE_URL ?? "http://localhost:5173"}
    />
  </StrictMode>,
);
