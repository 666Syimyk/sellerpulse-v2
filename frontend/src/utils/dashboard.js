export function buildFocusCards(products, hasPreliminaryData) {
  const noCostRows = products.filter((row) => row.status === "Нет себестоимости");
  const negativeRows = products.filter((row) => ["В минусе", "Минус с каждой продажи"].includes(row.status));
  const lowStockRows = products.filter((row) => row.status === "Заканчивается остаток" || row.action === "Пополнить остаток");
  const waitingFinanceRows = products.filter((row) => ["Ожидает расходы WB", "Нет данных WB"].includes(row.status));
  const lowMarginRows = products.filter((row) => row.status === "Низкая маржа");

  const cards = [];

  if (negativeRows.length) {
    cards.push({
      id: "negative",
      tone: "danger",
      label: "Риск прибыли",
      title: negativeRows.length === 1 ? "1 товар уходит в минус" : `${negativeRows.length} товаров уходят в минус`,
      text: previewRows(negativeRows, "Проверь цену, себестоимость и расходы по этим товарам в первую очередь."),
      button: "Показать убыточные",
      statusFilter: "loss",
      sortMode: "profit_asc",
    });
  }

  if (noCostRows.length) {
    cards.push({
      id: "no-cost",
      tone: "warning",
      label: "Нужно заполнить",
      title: noCostRows.length === 1 ? "1 товар без себестоимости" : `${noCostRows.length} товаров без себестоимости`,
      text: previewRows(noCostRows, "Без себестоимости мы не сможем посчитать чистую прибыль и маржу."),
      button: "Заполнить себестоимость",
      navigateTo: "costs",
    });
  }

  if (lowStockRows.length) {
    cards.push({
      id: "stock",
      tone: "warning",
      label: "Остатки",
      title: lowStockRows.length === 1 ? "1 товар скоро закончится" : `${lowStockRows.length} товаров скоро закончатся`,
      text: previewRows(lowStockRows, "Проверь поставку и остатки, чтобы не потерять продажи."),
      button: "Показать остатки",
      statusFilter: "Заканчивается остаток",
      sortMode: "attention",
    });
  }

  if (waitingFinanceRows.length && hasPreliminaryData) {
    cards.push({
      id: "finance",
      tone: "info",
      label: "WB данные",
      title: waitingFinanceRows.length === 1 ? "1 товар ждёт расходы WB" : `${waitingFinanceRows.length} товаров ждут расходы WB`,
      text: previewRows(waitingFinanceRows, "Продажи уже пришли, а точные расходы WB появятся после финансового отчёта."),
      button: "Показать ожидание WB",
      statusFilter: "waiting_wb",
      sortMode: "attention",
    });
  }

  if (lowMarginRows.length) {
    cards.push({
      id: "margin",
      tone: "neutral",
      label: "Маржа",
      title: lowMarginRows.length === 1 ? "1 товар с низкой маржой" : `${lowMarginRows.length} товаров с низкой маржой`,
      text: previewRows(lowMarginRows, "Сверь цену, логистику и рекламу, чтобы вернуть запас по прибыли."),
      button: "Показать маржу",
      statusFilter: "Низкая маржа",
      sortMode: "profit_asc",
    });
  }

  if (!cards.length) {
    cards.push({
      id: "calm",
      tone: "success",
      label: "Сегодня спокойно",
      title: "Критичных сигналов не найдено",
      text: "Все проданные сегодня товары выглядят нормально. Можно следить за динамикой продаж и расходами WB.",
      button: "Смотреть таблицу",
    });
  }

  return cards;
}

function previewRows(rows, fallback) {
  const preview = rows
    .slice(0, 2)
    .map((row) => row.vendor_code || row.name || `nmID ${row.nm_id}`)
    .join(", ");
  return preview ? `${preview}. ${fallback}` : fallback;
}

export function matchesStatusFilter(row, statusFilter) {
  if (statusFilter === "all") return true;
  if (statusFilter === "loss") return ["В минусе", "Минус с каждой продажи"].includes(row.status);
  if (statusFilter === "needs_cost") return row.status === "Нет себестоимости";
  if (statusFilter === "stock_risk") return row.status === "Заканчивается остаток" || row.action === "Пополнить остаток";
  if (statusFilter === "waiting_wb") return ["Ожидает расходы WB", "Нет данных WB"].includes(row.status);
  if (statusFilter === "low_margin") return row.status === "Низкая маржа";
  return row.status === statusFilter;
}

export function buildTodayPulseSummary(rows) {
  const soldQty = rows.reduce((sum, row) => sum + (row.sold_qty || 0), 0);
  const negativeCount = rows.filter((row) => ["В минусе", "Минус с каждой продажи"].includes(row.status)).length;
  const waitingCount = rows.filter((row) => ["Ожидает расходы WB", "Нет данных WB"].includes(row.status)).length;
  const noCostCount = rows.filter((row) => row.status === "Нет себестоимости").length;
  const stockRiskCount = rows.filter((row) => row.status === "Заканчивается остаток" || row.action === "Пополнить остаток").length;
  const profitableCount = rows.filter((row) => row.status === "В плюсе").length;
  const topProfitRow = [...rows]
    .filter((row) => row.profit != null)
    .sort((left, right) => nullableNumber(right.profit, Number.NEGATIVE_INFINITY) - nullableNumber(left.profit, Number.NEGATIVE_INFINITY))[0] || null;
  const topLossRow = [...rows]
    .filter((row) => row.profit != null)
    .sort((left, right) => nullableNumber(left.profit, Number.POSITIVE_INFINITY) - nullableNumber(right.profit, Number.POSITIVE_INFINITY))[0] || null;

  return {
    soldQty,
    negativeCount,
    waitingCount,
    noCostCount,
    stockRiskCount,
    profitableCount,
    topProfitRow,
    topLossRow,
  };
}

function nullableNumber(value, fallback) {
  return value === null || value === undefined ? fallback : Number(value);
}
