import { useState } from "react";
import type { PolicyKey } from "@ta-platform/api-client";
import { errorMessage, useClient, useLoad } from "../api";

const LABELS: Record<PolicyKey, string> = {
  ai_assistance: "AI assistant in the candidate workspace",
  ai_scoring: "AI-proposed scores",
  code_execution: "Running candidate code",
  live_monitoring: "Live monitoring",
  replay: "Interview replay",
};

export function Settings() {
  const client = useClient();
  const company = useLoad((c) => c.getCompany(), []);
  const policies = useLoad((c) => c.listPolicies(), []);
  const [error, setError] = useState<string>();

  async function toggle(key: PolicyKey, enabled: boolean) {
    setError(undefined);
    try {
      await client.setPolicy(key, enabled);
      policies.reload();
    } catch (e) {
      setError(errorMessage(e));
    }
  }

  return (
    <div className="page">
      <div className="page-head">
        <h1>Settings</h1>
        {company.data && <span className="muted">{company.data.name}</span>}
      </div>
      {error && <p role="alert" className="error">{error}</p>}
      {policies.error && <p role="alert" className="error">{policies.error}</p>}
      <section className="card">
        <h2>Policies</h2>
        {policies.data?.map((policy) => (
          <label key={policy.key} className="toggle">
            <input type="checkbox" checked={policy.enabled} onChange={(e) => toggle(policy.key, e.target.checked)} />
            {LABELS[policy.key] ?? policy.key}
          </label>
        ))}
      </section>
    </div>
  );
}
