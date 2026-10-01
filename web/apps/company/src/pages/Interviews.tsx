import { useState } from "react";
import type { Interview, Invite } from "@ta-platform/api-client";
import { errorMessage, useClient, useLoad } from "../api";
import { href } from "../route";

interface Props {
  /** Candidate app URL; invite links point here. */
  candidateUrl: string;
}

export function inviteLink(candidateUrl: string, token: string): string {
  return `${candidateUrl.replace(/\/+$/, "")}/?invite=${encodeURIComponent(token)}`;
}

export function Interviews({ candidateUrl }: Props) {
  const client = useClient();
  const interviews = useLoad((c) => c.listInterviews({ limit: 200 }), []);
  const [invites, setInvites] = useState<Record<string, Invite>>({});
  const [error, setError] = useState<string>();
  const [notice, setNotice] = useState<string>();

  async function act(run: () => Promise<unknown>, done?: string) {
    setError(undefined);
    setNotice(undefined);
    try {
      await run();
      if (done) setNotice(done);
      interviews.reload();
    } catch (e) {
      setError(errorMessage(e));
    }
  }

  return (
    <div className="page">
      <div className="page-head">
        <h1>Interviews</h1>
      </div>
      <NewInterview onCreate={(name, email) => act(() => client.createInterview({ candidate_name: name, candidate_email: email }))} />
      {error && <p role="alert" className="error">{error}</p>}
      {notice && <p role="status" className="notice">{notice}</p>}
      {interviews.error && <p role="alert" className="error">{interviews.error}</p>}
      {interviews.data && interviews.data.length === 0 && <p className="muted">No interviews yet.</p>}
      {interviews.data && interviews.data.length > 0 && (
        <table className="card">
          <thead>
            <tr>
              <th>Candidate</th>
              <th>Status</th>
              <th>Created</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {interviews.data.map((interview) => (
              <InterviewRow
                key={interview.id}
                interview={interview}
                invite={invites[interview.id]}
                candidateUrl={candidateUrl}
                onInvite={() =>
                  act(async () => {
                    const invite = await client.createInvite({ email: interview.candidate_email, interview_id: interview.id });
                    setInvites((all) => ({ ...all, [interview.id]: invite }));
                  })
                }
                onComplete={() => act(() => client.updateInterview(interview.id, { status: "completed" }))}
                onErase={() => {
                  if (window.confirm(`Request erasure of ${interview.candidate_name}'s interview data?`)) {
                    void act(() => client.requestErasure(interview.id), "Erasure requested. The data will be purged by the retention job.");
                  }
                }}
              />
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

function NewInterview({ onCreate }: { onCreate: (name: string, email: string) => Promise<void> }) {
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  return (
    <form
      className="card row"
      aria-label="New interview"
      onSubmit={async (e) => {
        e.preventDefault();
        await onCreate(name.trim(), email.trim());
        setName("");
        setEmail("");
      }}
    >
      <label>
        Candidate name
        <input value={name} onChange={(e) => setName(e.target.value)} required />
      </label>
      <label>
        Candidate email
        <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} required />
      </label>
      <button type="submit">Schedule interview</button>
    </form>
  );
}

interface RowProps {
  interview: Interview;
  invite?: Invite;
  candidateUrl: string;
  onInvite: () => void;
  onComplete: () => void;
  onErase: () => void;
}

function InterviewRow({ interview, invite, candidateUrl, onInvite, onComplete, onErase }: RowProps) {
  const terminal = interview.status === "completed" || interview.status === "cancelled";
  return (
    <>
      <tr>
        <td>
          <strong>{interview.candidate_name}</strong>
          <div className="muted">{interview.candidate_email}</div>
        </td>
        <td>
          <span className="pill">{interview.status.replace("_", " ")}</span>
          {interview.erase_requested_at && <span className="pill bad">erasure requested</span>}
        </td>
        <td>{new Date(interview.created_at).toLocaleDateString()}</td>
        <td className="actions">
          <a href={href({ page: "live", interviewId: interview.id })}>Watch live</a>
          <a href={href({ page: "replay", interviewId: interview.id })}>Replay</a>
          {!terminal && (
            <button className="link" onClick={onInvite}>
              Invite candidate
            </button>
          )}
          {!terminal && (
            <button className="link" onClick={onComplete}>
              Mark complete
            </button>
          )}
          {!interview.erase_requested_at && (
            <button className="link danger" onClick={onErase}>
              Request erasure
            </button>
          )}
        </td>
      </tr>
      {invite && (
        <tr className="invite">
          <td colSpan={4}>
            Send this one-time link to {invite.email} (expires {new Date(invite.expires_at).toLocaleString()}):{" "}
            <input readOnly value={inviteLink(candidateUrl, invite.token)} aria-label="Invite link" onFocus={(e) => e.target.select()} />
          </td>
        </tr>
      )}
    </>
  );
}
