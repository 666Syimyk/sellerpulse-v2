REQUIRED_PERMISSIONS = {
    "content": {"bit": 1, "title": "Контент", "affects": "товары и карточки"},
    "statistics": {"bit": 5, "title": "Статистика", "affects": "продажи, заказы и остатки"},
    "analytics": {"bit": 2, "title": "Аналитика", "affects": "аналитические показатели"},
    "finance": {"bit": 13, "title": "Финансы", "affects": "комиссии, логистика, хранение, штрафы и удержания"},
    "promotion": {"bit": 6, "title": "Продвижение", "affects": "реклама и ДРР"},
    "prices": {"bit": 3, "title": "Цены и скидки", "affects": "цены, скидки и СПП"},
    "supplies": {"bit": 10, "title": "Поставки", "affects": "поставки и остатки"},
    "returns": {"bit": 11, "title": "Возвраты", "affects": "возвраты и процент выкупа"},
}


def permission_report(scope_mask: int) -> dict:
    items = {}
    for code, meta in REQUIRED_PERMISSIONS.items():
        items[code] = {
            "title": meta["title"],
            "has_access": bool(scope_mask & (1 << meta["bit"])),
            "affects": meta["affects"],
        }
    missing = [item["title"] for item in items.values() if not item["has_access"]]
    affected = [item["affects"] for item in items.values() if not item["has_access"]]
    return {"items": items, "missing": missing, "affected": affected}
