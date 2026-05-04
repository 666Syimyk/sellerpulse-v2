from datetime import date, timedelta

_MIN_DATE = date(2000, 1, 1)
_MAX_DATE = date(2099, 12, 31)


def period_dates(period: str) -> tuple[date, date]:
    today = date.today()
    if period == "today":
        return today, today
    if period == "week":
        return today - timedelta(days=6), today
    if period == "month":
        return today.replace(day=1), today
    if period == "last_month":
        first_this_month = today.replace(day=1)
        last_prev_month = first_this_month - timedelta(days=1)
        return last_prev_month.replace(day=1), last_prev_month
    if period == "report":
        return _MIN_DATE, _MAX_DATE
    raise ValueError("Unknown period")
