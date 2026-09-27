import { describe, expect, it } from "vitest";
import { parseSSE } from "../src/sse";
import { streamOf } from "./helpers";

async function collect(chunks: string[]) {
  const out = [];
  for await (const event of parseSSE(streamOf(chunks))) out.push(event);
  return out;
}

describe("parseSSE", () => {
  it("parses named events and joins multi-line data", async () => {
    expect(await collect(["event: delta\ndata: {\"text\":\"a\"}\n\n", "data: one\ndata: two\n\n"])).toEqual([
      { event: "delta", data: '{"text":"a"}' },
      { event: "message", data: "one\ntwo" },
    ]);
  });

  it("handles events split across chunks, CRLF, comments and unknown fields", async () => {
    expect(await collect([": ping\n\nev", "ent: done\r\nid: 3\r\nretry: 5\r\nda", "ta: {}\r", "\n\r\n"])).toEqual([
      { event: "done", data: "{}", id: "3" },
    ]);
  });

  it("decodes multi-byte characters split across chunks", async () => {
    const bytes = new TextEncoder().encode("data: é\n\n");
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(bytes.slice(0, 7));
        controller.enqueue(bytes.slice(7));
        controller.close();
      },
    });
    const events = [];
    for await (const event of parseSSE(stream)) events.push(event);
    expect(events).toEqual([{ event: "message", data: "é" }]);
  });

  it("drops an unterminated trailing event instead of hanging", async () => {
    expect(await collect(["event: delta\ndata: {}"])).toEqual([]);
  });
});

describe("parseSSE ids", () => {
  it("attaches an event's id only when it has one", async () => {
    const events = [];
    for await (const e of parseSSE(streamOf(["id: 7\nevent: event\ndata: {}\n\n", "event: caught_up\ndata: {}\n\n"]))) {
      events.push(e);
    }
    expect(events).toEqual([
      { event: "event", data: "{}", id: "7" },
      { event: "caught_up", data: "{}" },
    ]);
    expect("id" in events[1]).toBe(false);
  });
});
