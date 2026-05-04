import base64
import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx
from sqlalchemy.orm import Session

from models.entities import WbRawResponse
from wb_api.permissions import REQUIRED_PERMISSIONS, permission_report

logger = logging.getLogger(__name__)


class WbApiError(Exception):
    def __init__(self, message: str, status_code: int | None = None, endpoint: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.endpoint = endpoint


class WbRateLimited(WbApiError):
    pass


class WbInvalidToken(WbApiError):
    pass


class WbLimitedPermission(WbApiError):
    pass


@dataclass
class TokenCheck:
    status: str
    shop_name: str | None
    permissions: dict
    api_errors: list[str]


def _decode_jwt_payload(token: str) -> dict:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload.encode()))
    except Exception as exc:
        raise WbInvalidToken("WB token is not a valid JWT") from exc


PING_ENDPOINTS = {
    "content": "https://content-api.wildberries.ru/ping",
    "statistics": "https://statistics-api.wildberries.ru/ping",
    "analytics": "https://seller-analytics-api.wildberries.ru/ping",
    "finance": "https://finance-api.wildberries.ru/ping",
    "promotion": "https://advert-api.wildberries.ru/ping",
    "prices": "https://discounts-prices-api.wildberries.ru/ping",
    "supplies": "https://supplies-api.wildberries.ru/ping",
    "returns": "https://returns-api.wildberries.ru/ping",
}


class WbClient:
    def __init__(self, token: str, db: Session | None = None, user_id: int | None = None, wb_token_id: int | None = None):
        self.token = normalize_token(token)
        self.headers = {"Authorization": self.token}
        self.db = db
        self.user_id = user_id
        self.wb_token_id = wb_token_id

    async def check_token(self, probe_permissions: bool = True) -> TokenCheck:
        payload = _decode_jwt_payload(self.token)
        scope_mask = int(payload.get("s") or 0)
        bitmask_permissions = permission_report(scope_mask)

        items = {}
        api_errors: list[str] = []
        ping_statuses: list[str] = []
        for code, meta in REQUIRED_PERMISSIONS.items():
            ping_status = await self._probe("GET", PING_ENDPOINTS[code], timeout=10)
            ping_statuses.append(ping_status)
            jwt_has_access = bitmask_permissions["items"].get(code, {}).get("has_access", False)
            has_access = jwt_has_access and ping_status not in ("invalid", "limited")
            if ping_status == "api_error":
                api_errors.append(meta["title"])
            items[code] = {
                "title": meta["title"],
                "has_access": has_access,
                "affects": meta["affects"],
                "ping_status": ping_status,
                "jwt_has_access": jwt_has_access,
            }

        missing = [item["title"] for item in items.values() if not item["has_access"]]
        affected = [item["affects"] for item in items.values() if not item["has_access"]]
        permissions = {"items": items, "missing": missing, "affected": affected}

        if any(status == "invalid" for status in ping_statuses):
            status = "invalid"
        elif any(status == "rate_limited" for status in ping_statuses):
            status = "rate_limited"
        elif any(status == "api_error" for status in ping_statuses):
            status = "api_error"
        elif missing:
            status = "limited"
        else:
            status = "active"

        seller_id = payload.get("sid")
        shop_name = f"WB кабинет {str(seller_id)[:8]}" if seller_id else "WB кабинет"
        return TokenCheck(status=status, shop_name=shop_name, permissions=permissions, api_errors=api_errors)

    async def _probe(self, method: str, url: str, params: dict | None = None, json_body: Any | None = None, timeout: int = 30) -> str:
        try:
            await self._request(method, url, params=params, json_body=json_body, timeout=timeout)
            return "ok"
        except WbRateLimited:
            return "rate_limited"
        except WbInvalidToken:
            return "invalid"
        except WbLimitedPermission:
            return "limited"
        except WbApiError:
            return "api_error"

    async def _request(
        self,
        method: str,
        url: str,
        params: dict | None = None,
        json_body: Any | None = None,
        timeout: int = 30,
    ) -> list[dict] | dict:
        request_data = {"method": method, "params": params or {}, "json": json_body}
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.request(method, url, headers=self.headers, params=params, json=json_body)
            payload = _safe_json(response)
            error_message = None if response.status_code < 400 else _response_error_message(payload, response.status_code)
            self._log_raw(url, request_data, payload, response.status_code, error_message)
            logger.info("WB API request method=%s endpoint=%s status_code=%s", method, url, response.status_code)

            if response.status_code == 429:
                raise WbRateLimited("WB временно ограничил запросы", 429, url)
            if response.status_code == 401:
                raise WbInvalidToken("WB вернул 401 Unauthorized", 401, url)
            if response.status_code == 403:
                raise WbLimitedPermission("Недостаточно прав токена WB", 403, url)
            if response.status_code >= 500:
                raise WbApiError("WB API временно недоступен", response.status_code, url)
            if response.status_code >= 400:
                raise WbApiError(f"WB API вернул ошибку {response.status_code}", response.status_code, url)
            return payload
        except httpx.TimeoutException as exc:
            self._log_raw(url, request_data, None, None, "WB API timeout")
            logger.warning("WB API timeout method=%s endpoint=%s", method, url)
            raise WbApiError("WB API timeout", None, url) from exc
        except httpx.HTTPError as exc:
            self._log_raw(url, request_data, None, None, str(exc))
            logger.warning("WB API request error method=%s endpoint=%s error=%s", method, url, exc)
            raise WbApiError("WB API request error", None, url) from exc

    def _log_raw(self, endpoint: str, request_data: dict, response_json: Any, status_code: int | None, error: str | None) -> None:
        if not self.db:
            return
        self.db.add(
            WbRawResponse(
                user_id=self.user_id,
                wb_token_id=self.wb_token_id,
                endpoint=endpoint,
                request_params_json=request_data,
                response_json=response_json,
                status_code=status_code,
                error_message=error,
            )
        )
        self.db.flush()

    async def fetch_products(self) -> list[dict]:
        cards: list[dict] = []
        cursor: dict[str, Any] = {"limit": 100}
        while True:
            payload = {"settings": {"cursor": cursor, "filter": {"withPhoto": -1}}}
            response = await self._request(
                "POST",
                "https://content-api.wildberries.ru/content/v2/get/cards/list",
                json_body=payload,
            )
            batch = response.get("cards", []) if isinstance(response, dict) else []
            cards.extend(batch)
            response_cursor = response.get("cursor", {}) if isinstance(response, dict) else {}
            if int(response_cursor.get("total") or 0) < cursor["limit"]:
                break
            if not response_cursor.get("updatedAt") or not response_cursor.get("nmID"):
                break
            cursor = {"limit": 100, "updatedAt": response_cursor["updatedAt"], "nmID": response_cursor["nmID"]}
        return cards

    async def fetch_sales(self, date_from: date, date_to: date) -> list[dict]:
        response = await self._request(
            "GET",
            "https://statistics-api.wildberries.ru/api/v1/supplier/sales",
            {"dateFrom": date_from.isoformat(), "flag": 0},
        )
        return response if isinstance(response, list) else []

    async def fetch_orders(self, date_from: date, date_to: date) -> list[dict]:
        response = await self._request(
            "GET",
            "https://statistics-api.wildberries.ru/api/v1/supplier/orders",
            {"dateFrom": date_from.isoformat(), "flag": 0},
        )
        return response if isinstance(response, list) else []

    async def fetch_stocks(self) -> list[dict]:
        response = await self._request(
            "GET",
            "https://statistics-api.wildberries.ru/api/v1/supplier/stocks",
            {"dateFrom": "2019-06-20"},
        )
        return response if isinstance(response, list) else []

    async def fetch_financial_report(self, date_from: date, date_to: date) -> list[dict]:
        finance = FinanceReportsClient(self)
        return await finance.fetch_sales_report_details(date_from, date_to)

    async def fetch_advertising(self, date_from: date, date_to: date) -> list[dict]:
        campaigns_response = await self._request("GET", "https://advert-api.wildberries.ru/adv/v1/promotion/count")
        campaign_ids = _campaign_ids(campaigns_response)
        rows: list[dict] = []
        for index in range(0, len(campaign_ids), 50):
            chunk = campaign_ids[index : index + 50]
            if not chunk:
                continue
            response = await self._request(
                "GET",
                "https://advert-api.wildberries.ru/adv/v3/fullstats",
                {
                    "ids": ",".join(str(item) for item in chunk),
                    "beginDate": date_from.isoformat(),
                    "endDate": date_to.isoformat(),
                },
            )
            if isinstance(response, list):
                rows.extend(response)
        return rows


class FinanceReportsClient:
    def __init__(self, wb: WbClient):
        self.wb = wb

    async def fetch_sales_report_details(self, date_from: date, date_to: date) -> list[dict]:
        try:
            return await self._fetch_new_period_report(date_from, date_to)
        except (WbInvalidToken, WbLimitedPermission, WbRateLimited):
            raise
        except WbApiError:
            return await self._fetch_legacy_report(date_from, date_to)

    async def _fetch_new_period_report(self, date_from: date, date_to: date) -> list[dict]:
        rows: list[dict] = []
        rrd_id = 0
        fields = [
            "rrdId",
            "nmId",
            "vendorCode",
            "title",
            "brandName",
            "subjectName",
            "docTypeName",
            "quantity",
            "saleDt",
            "rrDate",
            "orderDt",
            "retailAmount",
            "retailPriceWithDisc",
            "forPay",
            "ppvzSalesCommission",
            "ppvzReward",
            "deliveryService",
            "rebillLogisticCost",
            "paidStorage",
            "returnAmount",
            "acquiringFee",
            "sellerPromo",
            "penalty",
            "deduction",
            "additionalPayment",
            "paidAcceptance",
        ]
        while True:
            body = {
                "dateFrom": date_from.isoformat(),
                "dateTo": date_to.isoformat(),
                "fields": fields,
                "limit": 100000,
                "rrdId": rrd_id,
                "period": "daily",
            }
            response = await self.wb._request(
                "POST",
                "https://finance-api.wildberries.ru/api/finance/v1/sales-reports/detailed",
                json_body=body,
            )
            batch = _extract_rows(response)
            if not batch:
                break
            rows.extend(batch)
            next_rrd_id = int(batch[-1].get("rrdId") or rrd_id)
            if next_rrd_id == rrd_id or len(batch) < 100000:
                break
            rrd_id = next_rrd_id
        return rows

    async def _fetch_legacy_report(self, date_from: date, date_to: date) -> list[dict]:
        rows: list[dict] = []
        rrdid = 0
        while True:
            batch = await self.wb._request(
                "GET",
                "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod",
                {"dateFrom": date_from.isoformat(), "dateTo": date_to.isoformat(), "rrdid": rrdid, "limit": 100000},
            )
            if not isinstance(batch, list) or not batch:
                break
            rows.extend(batch)
            next_rrdid = int(batch[-1].get("rrd_id") or rrdid)
            if next_rrdid == rrdid or len(batch) < 100000:
                break
            rrdid = next_rrdid
        return rows


def _safe_json(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return {"text": response.text[:5000]}


def _response_error_message(payload: Any, status_code: int) -> str:
    if isinstance(payload, dict):
        for key in ("message", "detail", "errorText", "error", "title"):
            value = payload.get(key)
            if value:
                return str(value)
    return f"WB API вернул HTTP {status_code}"


def normalize_token(token: str) -> str:
    token = token.strip()
    if token.lower().startswith("bearer "):
        return token[7:].strip()
    return token


def _extract_rows(response: Any) -> list[dict]:
    if isinstance(response, list):
        return response
    if not isinstance(response, dict):
        return []
    for key in ("data", "reports", "rows", "details"):
        value = response.get(key)
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            nested = _extract_rows(value)
            if nested:
                return nested
    return []


def _campaign_ids(response: Any) -> list[int]:
    ids: list[int] = []
    if not isinstance(response, dict):
        return ids
    for group in response.get("adverts", []):
        for advert in group.get("advert_list", []):
            advert_id = advert.get("advertId")
            if advert_id is not None:
                ids.append(int(advert_id))
    return ids
