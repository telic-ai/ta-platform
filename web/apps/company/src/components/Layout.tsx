import type { ReactNode } from "react";
import { href, type Route } from "../route";

interface Props {
  route: Route;
  onSignOut: () => void;
  children: ReactNode;
}

const NAV: { label: string; route: Route; active: Route["page"][] }[] = [
  { label: "Dashboard", route: { page: "dashboard" }, active: ["dashboard"] },
  { label: "Interviews", route: { page: "interviews" }, active: ["interviews", "live", "replay"] },
  { label: "Settings", route: { page: "settings" }, active: ["settings"] },
];

export function Layout({ route, onSignOut, children }: Props) {
  return (
    <div className="shell">
      <header className="topbar">
        <strong className="brand">Lucid AI</strong>
        <nav aria-label="Main">
          {NAV.map((item) => (
            <a key={item.label} href={href(item.route)} aria-current={item.active.includes(route.page) ? "page" : undefined}>
              {item.label}
            </a>
          ))}
        </nav>
        <button className="link" onClick={onSignOut}>
          Sign out
        </button>
      </header>
      <main className="content">{children}</main>
    </div>
  );
}
