import { useState } from "react";
import { WORKSPACE_STARTERS } from "@ta-platform/api-client";

interface Props {
  files: Record<string, string>;
  /**
   * The file to show; follows changes unless the viewer picks one. Before
   * any edit, the workspace's default (Python) file.
   */
  active?: string;
}

/** Read-only file tabs with the selected file's content. */
export function CodeView({ files, active }: Props) {
  const names = Object.keys(files).sort();
  const [picked, setPicked] = useState<string>();
  const fallback = files[WORKSPACE_STARTERS.python.file] !== undefined ? WORKSPACE_STARTERS.python.file : names[0];
  const shown = picked && files[picked] !== undefined ? picked : (active ?? fallback);
  if (names.length === 0) return <p className="muted">No files yet.</p>;
  return (
    <section className="code" aria-label="Candidate code">
      <div role="tablist" className="tabs">
        {names.map((name) => (
          <button key={name} role="tab" aria-selected={name === shown} onClick={() => setPicked(name)}>
            {name}
          </button>
        ))}
      </div>
      <pre data-testid="code">
        <code>{files[shown ?? ""]}</code>
      </pre>
    </section>
  );
}
