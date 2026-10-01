import { useEffect, useState } from "react";

export type Route =
  | { page: "dashboard" }
  | { page: "interviews" }
  | { page: "live"; interviewId: string }
  | { page: "replay"; interviewId: string }
  | { page: "settings" };

/** Parses a location hash such as "#/interviews/<id>/live". */
export function parseRoute(hash: string): Route {
  const parts = hash.replace(/^#\/?/, "").split("/").filter(Boolean).map(decodeURIComponent);
  if (parts[0] === "interviews" && parts[1] && parts[2] === "live") return { page: "live", interviewId: parts[1] };
  if (parts[0] === "interviews" && parts[1] && parts[2] === "replay") return { page: "replay", interviewId: parts[1] };
  if (parts[0] === "interviews") return { page: "interviews" };
  if (parts[0] === "settings") return { page: "settings" };
  return { page: "dashboard" };
}

export function href(route: Route): string {
  switch (route.page) {
    case "live":
    case "replay":
      return `#/interviews/${encodeURIComponent(route.interviewId)}/${route.page}`;
    default:
      return `#/${route.page}`;
  }
}

export function useRoute(): Route {
  const [route, setRoute] = useState(() => parseRoute(window.location.hash));
  useEffect(() => {
    const onChange = () => setRoute(parseRoute(window.location.hash));
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);
  return route;
}
