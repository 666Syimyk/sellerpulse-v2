export function money(value) {
  if (value === null || value === undefined) return "Нет данных WB";
  return new Intl.NumberFormat("ru-RU", { style: "currency", currency: "RUB", maximumFractionDigits: 0 }).format(value);
}

export function number(value, suffix = "") {
  if (value === null || value === undefined) return "Нет данных WB";
  return `${new Intl.NumberFormat("ru-RU").format(value)}${suffix}`;
}

export function percent(value) {
  if (value === null || value === undefined) return "Нет данных WB";
  return `${new Intl.NumberFormat("ru-RU", { maximumFractionDigits: 1 }).format(value)}%`;
}

export function dateTime(value) {
  if (!value) return "Нет синхронизации";
  return new Intl.DateTimeFormat("ru-RU", { dateStyle: "medium", timeStyle: "short" }).format(new Date(value));
}
