import { describe, expect, it } from "vitest";
import { fillDays, niceTicks } from "../src/dailyChart";
import { inviteLink } from "../src/pages/Interviews";
import { href, parseRoute } from "../src/route";

describe("routes", () => {
  it("parses and builds hashes", () => {
    expect(parseRoute("")).toEqual({ page: "dashboard" });
    expect(parseRoute("#/nowhere")).toEqual({ page: "dashboard" });
    expect(parseRoute("#/interviews")).toEqual({ page: "interviews" });
    expect(parseRoute("#/settings")).toEqual({ page: "settings" });
    expect(parseRoute("#/interviews/a%2Fb/live")).toEqual({ page: "live", interviewId: "a/b" });
    expect(parseRoute("#/interviews/x/replay")).toEqual({ page: "replay", interviewId: "x" });
    for (const route of [
      { page: "dashboard" },
      { page: "interviews" },
      { page: "settings" },
      { page: "live", interviewId: "a/b" },
      { page: "replay", interviewId: "x" },
    ] as const) {
      expect(parseRoute(href(route))).toEqual(route);
    }
  });
});

describe("fillDays", () => {
  it("sums per day and fills gaps with zero", () => {
    expect(
      fillDays(
        [
          { day: "2026-09-02", event_type: "a", events: 2 },
          { day: "2026-09-02", event_type: "b", events: 3 },
          { day: "2026-09-04", event_type: "a", events: 1 },
        ],
        "2026-09-01",
        4,
      ),
    ).toEqual([
      { day: "2026-09-01", events: 0 },
      { day: "2026-09-02", events: 5 },
      { day: "2026-09-03", events: 0 },
      { day: "2026-09-04", events: 1 },
    ]);
  });

  it("crosses month ends", () => {
    expect(fillDays([], "2026-09-30", 2).map((d) => d.day)).toEqual(["2026-09-30", "2026-10-01"]);
  });
});

describe("niceTicks", () => {
  it("covers the maximum with round steps from zero", () => {
    expect(niceTicks(1)).toEqual([0, 1]);
    expect(niceTicks(7)).toEqual([0, 2, 4, 6, 8]);
    expect(niceTicks(40)).toEqual([0, 10, 20, 30, 40]);
    expect(niceTicks(1234).at(-1)).toBeGreaterThanOrEqual(1234);
    expect(niceTicks(1234)[0]).toBe(0);
  });
});

describe("inviteLink", () => {
  it("encodes the token onto the candidate app", () => {
    expect(inviteLink("http://c.test/", "a/b+c")).toBe("http://c.test/?invite=a%2Fb%2Bc");
  });
});
