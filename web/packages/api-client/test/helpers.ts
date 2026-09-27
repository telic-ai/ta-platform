/** A ReadableStream that yields the given chunks, optionally pausing. */
export function streamOf(chunks: string[], hold?: Promise<void>): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream({
    async start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      if (hold) await hold;
      controller.close();
    },
  });
}

export interface Call {
  url: string;
  init: RequestInit;
}

/** A fetch stub that records calls and answers from a handler. */
export function fakeFetch(handler: (url: string, init: RequestInit) => Response | Promise<Response>) {
  const calls: Call[] = [];
  const fn = (async (input: RequestInfo | URL, init: RequestInit = {}) => {
    const url = String(input);
    calls.push({ url, init });
    return handler(url, init);
  }) as typeof fetch;
  return { fetch: fn, calls };
}

export const json = (status: number, body: unknown) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
