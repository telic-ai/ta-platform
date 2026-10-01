import { createContext, useCallback, useContext, useEffect, useState } from "react";
import { CompanyApiError, type CompanyClient } from "@ta-platform/api-client";

export const ClientContext = createContext<CompanyClient | null>(null);

export function useClient(): CompanyClient {
  const client = useContext(ClientContext);
  if (!client) throw new Error("useClient outside ClientContext");
  return client;
}

export interface Loaded<T> {
  data?: T;
  error?: string;
  loading: boolean;
  reload: () => void;
}

/** Loads data with the client, re-running when deps change or on reload(). */
export function useLoad<T>(load: (client: CompanyClient) => Promise<T>, deps: unknown[]): Loaded<T> {
  const client = useClient();
  const [state, setState] = useState<{ data?: T; error?: string; loading: boolean }>({ loading: true });
  const [version, setVersion] = useState(0);
  useEffect(() => {
    let live = true;
    setState((s) => ({ ...s, loading: true }));
    load(client).then(
      (data) => live && setState({ data, loading: false }),
      (error: unknown) => live && setState({ error: errorMessage(error), loading: false }),
    );
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, version, ...deps]);
  const reload = useCallback(() => setVersion((v) => v + 1), []);
  return { ...state, reload };
}

export function errorMessage(error: unknown): string {
  if (error instanceof CompanyApiError) {
    if (error.status === 403) return `Not allowed: ${error.message}`;
    if (error.status === 404) return "Not found.";
    if (error.status >= 500) return "The service is unavailable. Try again shortly.";
    return error.message;
  }
  return error instanceof Error ? error.message : String(error);
}
