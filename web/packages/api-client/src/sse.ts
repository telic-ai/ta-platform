/** One Server-Sent Event. id is set only when the event carried one. */
export interface SSEEvent {
  event: string;
  data: string;
  id?: string;
}

/**
 * Parses a text/event-stream body. EventSource cannot POST or send an
 * Authorization header, so streamed endpoints are read with fetch and
 * parsed here.
 */
export async function* parseSSE(body: ReadableStream<Uint8Array>): AsyncGenerator<SSEEvent> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let event = "";
  let data: string[] = [];
  let id: string | undefined;
  let seen = false;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      buffer += done ? decoder.decode() : decoder.decode(value, { stream: true });
      let newline: number;
      while ((newline = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, newline).replace(/\r$/, "");
        buffer = buffer.slice(newline + 1);
        if (line === "") {
          if (seen) yield { event: event || "message", data: data.join("\n"), ...(id === undefined ? {} : { id }) };
          event = "";
          data = [];
          id = undefined;
          seen = false;
          continue;
        }
        if (line.startsWith(":")) continue;
        const colon = line.indexOf(":");
        const field = colon < 0 ? line : line.slice(0, colon);
        let fieldValue = colon < 0 ? "" : line.slice(colon + 1);
        if (fieldValue.startsWith(" ")) fieldValue = fieldValue.slice(1);
        if (field === "event") {
          event = fieldValue;
          seen = true;
        } else if (field === "data") {
          data.push(fieldValue);
          seen = true;
        } else if (field === "id") {
          id = fieldValue;
          seen = true;
        }
      }
      // An unterminated last line or event is incomplete and dropped.
      if (done) break;
    }
  } finally {
    reader.releaseLock();
  }
}
