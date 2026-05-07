import { describe, expect, it } from "vitest";

import { buildFocusCards, buildTodayPulseSummary, matchesStatusFilter } from "./dashboard";

const rows = [
  {
    nm_id: 1,
    vendor_code: "LOSS-1",
    name: "Убыточный товар",
    status: "В минусе",
    sold_qty: 2,
    action: "Проверить цену и расходы",
    profit: -300,
  },
  {
    nm_id: 2,
    vendor_code: "WAIT-1",
    name: "Ждёт WB",
    status: "Ожидает расходы WB",
    sold_qty: 1,
    action: "Проверить позже",
    profit: null,
  },
  {
    nm_id: 3,
    vendor_code: "COST-1",
    name: "Без себестоимости",
    status: "Нет себестоимости",
    sold_qty: 1,
    action: "Указать себестоимость",
    profit: null,
  },
  {
    nm_id: 4,
    vendor_code: "STOCK-1",
    name: "Заканчивается",
    status: "Заканчивается остаток",
    sold_qty: 3,
    action: "Пополнить остаток",
    profit: 450,
  },
  {
    nm_id: 5,
    vendor_code: "MARGIN-1",
    name: "Низкая маржа",
    status: "Низкая маржа",
    sold_qty: 2,
    action: "Поднять цену",
    profit: 40,
  },
  {
    nm_id: 6,
    vendor_code: "PLUS-1",
    name: "Лидер",
    status: "В плюсе",
    sold_qty: 5,
    action: "Контролировать остатки",
    profit: 900,
  },
];

describe("matchesStatusFilter", () => {
  it("matches grouped loss filter", () => {
    expect(matchesStatusFilter(rows[0], "loss")).toBe(true);
    expect(matchesStatusFilter(rows[5], "loss")).toBe(false);
  });

  it("matches waiting WB and stock risk filters", () => {
    expect(matchesStatusFilter(rows[1], "waiting_wb")).toBe(true);
    expect(matchesStatusFilter(rows[3], "stock_risk")).toBe(true);
    expect(matchesStatusFilter(rows[4], "low_margin")).toBe(true);
  });
});

describe("buildTodayPulseSummary", () => {
  it("builds aggregate summary and leaders", () => {
    const summary = buildTodayPulseSummary(rows);

    expect(summary.soldQty).toBe(14);
    expect(summary.negativeCount).toBe(1);
    expect(summary.waitingCount).toBe(1);
    expect(summary.noCostCount).toBe(1);
    expect(summary.stockRiskCount).toBe(1);
    expect(summary.profitableCount).toBe(1);
    expect(summary.topProfitRow.vendor_code).toBe("PLUS-1");
    expect(summary.topLossRow.vendor_code).toBe("LOSS-1");
  });
});

describe("buildFocusCards", () => {
  it("returns the expected priority cards", () => {
    const cards = buildFocusCards(rows, true);
    const ids = cards.map((card) => card.id);

    expect(ids).toEqual(["negative", "no-cost", "stock", "finance", "margin"]);
    expect(cards.find((card) => card.id === "negative")).toMatchObject({
      statusFilter: "loss",
      sortMode: "profit_asc",
    });
    expect(cards.find((card) => card.id === "finance")).toMatchObject({
      statusFilter: "waiting_wb",
      sortMode: "attention",
    });
  });

  it("returns calm card when there are no issues", () => {
    const cards = buildFocusCards([{ nm_id: 10, status: "В плюсе", sold_qty: 1, profit: 100 }], false);

    expect(cards).toHaveLength(1);
    expect(cards[0].id).toBe("calm");
  });
});
