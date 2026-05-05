from datetime import date, datetime
from enum import Enum

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Integer, JSON, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database import Base


class SubscriptionStatus(str, Enum):
    trial = "trial"
    active = "active"
    expired = "expired"
    cancelled = "cancelled"


class TokenStatus(str, Enum):
    active = "active"
    limited = "limited"
    invalid = "invalid"
    rate_limited = "rate_limited"
    api_error = "api_error"


class DataAccuracy(str, Enum):
    exact = "Точные данные WB"
    estimated = "Оценочный расчёт"
    missing = "Нет данных WB"


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(160))
    email: Mapped[str] = mapped_column(String(320), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    wb_tokens: Mapped[list["WbToken"]] = relationship(back_populates="user")
    subscription: Mapped["Subscription | None"] = relationship(back_populates="user", uselist=False)

    @property
    def wb_token(self) -> "WbToken | None":
        active_tokens = [
            token
            for token in self.wb_tokens
            if token.is_active and token.token_status != TokenStatus.invalid.value
        ]
        return max(active_tokens, key=lambda token: token.id) if active_tokens else None


class WbToken(Base):
    __tablename__ = "wb_tokens"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    encrypted_token: Mapped[str] = mapped_column(String(4096))
    shop_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    token_status: Mapped[str] = mapped_column(String(40), default=TokenStatus.invalid.value)
    permissions: Mapped[dict] = mapped_column(JSON, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    sync_in_progress: Mapped[bool] = mapped_column(Boolean, default=False)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_sync_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    user: Mapped[User] = relationship(back_populates="wb_tokens")


class Product(Base):
    __tablename__ = "products"
    __table_args__ = (UniqueConstraint("user_id", "nm_id", name="uq_products_user_nm"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    wb_token_id: Mapped[int | None] = mapped_column(ForeignKey("wb_tokens.id"), index=True, nullable=True)
    nm_id: Mapped[int] = mapped_column(index=True)
    vendor_code: Mapped[str] = mapped_column(String(120), default="")
    name: Mapped[str] = mapped_column(String(500), default="")
    brand: Mapped[str | None] = mapped_column(String(255), nullable=True)
    category: Mapped[str | None] = mapped_column(String(255), nullable=True)
    cost_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    tax_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class Sale(Base):
    __tablename__ = "sales"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    wb_token_id: Mapped[int] = mapped_column(ForeignKey("wb_tokens.id"), index=True)
    nm_id: Mapped[int] = mapped_column(index=True)
    date: Mapped[date] = mapped_column(Date, index=True)
    quantity: Mapped[int] = mapped_column(Integer, default=0)
    total_before_spp: Mapped[float | None] = mapped_column(Float, nullable=True)
    spp_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_after_spp: Mapped[float | None] = mapped_column(Float, nullable=True)
    to_pay: Mapped[float | None] = mapped_column(Float, nullable=True)


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    wb_token_id: Mapped[int] = mapped_column(ForeignKey("wb_tokens.id"), index=True)
    nm_id: Mapped[int] = mapped_column(index=True)
    date: Mapped[date] = mapped_column(Date, index=True)
    quantity: Mapped[int] = mapped_column(Integer, default=0)
    total_before_spp: Mapped[float | None] = mapped_column(Float, nullable=True)
    spp_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_after_spp: Mapped[float | None] = mapped_column(Float, nullable=True)


class Expense(Base):
    __tablename__ = "expenses"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    wb_token_id: Mapped[int] = mapped_column(ForeignKey("wb_tokens.id"), index=True)
    nm_id: Mapped[int] = mapped_column(index=True)
    date: Mapped[date] = mapped_column(Date, index=True)
    commission: Mapped[float | None] = mapped_column(Float, nullable=True)
    logistics: Mapped[float | None] = mapped_column(Float, nullable=True)
    storage: Mapped[float | None] = mapped_column(Float, nullable=True)
    returns: Mapped[float | None] = mapped_column(Float, nullable=True)
    returns_qty: Mapped[int | None] = mapped_column(Integer, nullable=True)
    acquiring: Mapped[float | None] = mapped_column(Float, nullable=True)
    spa: Mapped[float | None] = mapped_column(Float, nullable=True)
    advertising: Mapped[float | None] = mapped_column(Float, nullable=True)
    tax: Mapped[float | None] = mapped_column(Float, nullable=True)
    penalties: Mapped[float | None] = mapped_column(Float, nullable=True)
    deductions: Mapped[float | None] = mapped_column(Float, nullable=True)
    other_expenses: Mapped[float | None] = mapped_column(Float, nullable=True)
    data_accuracy: Mapped[str] = mapped_column(String(80), default=DataAccuracy.missing.value)


class Stock(Base):
    __tablename__ = "stocks"
    __table_args__ = (UniqueConstraint("wb_token_id", "nm_id", name="uq_stocks_token_nm"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    wb_token_id: Mapped[int] = mapped_column(ForeignKey("wb_tokens.id"), index=True)
    nm_id: Mapped[int] = mapped_column(index=True)
    quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class DashboardCache(Base):
    __tablename__ = "dashboard_cache"
    __table_args__ = (UniqueConstraint("wb_token_id", "period", name="uq_dashboard_token_period"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    wb_token_id: Mapped[int] = mapped_column(ForeignKey("wb_tokens.id"), index=True)
    period: Mapped[str] = mapped_column(String(40))
    data_json: Mapped[dict] = mapped_column(JSON)
    calculated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class WbRawResponse(Base):
    __tablename__ = "wb_raw_responses"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), index=True, nullable=True)
    wb_token_id: Mapped[int | None] = mapped_column(ForeignKey("wb_tokens.id"), index=True, nullable=True)
    endpoint: Mapped[str] = mapped_column(String(500), index=True)
    request_params_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    response_json: Mapped[dict | list | None] = mapped_column(JSON, nullable=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class FinancialReport(Base):
    __tablename__ = "financial_reports"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    file_name: Mapped[str] = mapped_column(String(500))
    period_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    period_end: Mapped[date | None] = mapped_column(Date, nullable=True)
    rows_count: Mapped[int] = mapped_column(Integer, default=0)
    products_count: Mapped[int] = mapped_column(Integer, default=0)
    manual_tax: Mapped[float | None] = mapped_column(Float, nullable=True)
    manual_advertising: Mapped[float | None] = mapped_column(Float, nullable=True)
    manual_other_expenses: Mapped[float | None] = mapped_column(Float, nullable=True)
    validation_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    source_rows_json: Mapped[list | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class FinancialReportItem(Base):
    __tablename__ = "financial_report_items"

    id: Mapped[int] = mapped_column(primary_key=True)
    report_id: Mapped[int] = mapped_column(ForeignKey("financial_reports.id"), index=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    nm_id: Mapped[int | None] = mapped_column(Integer, index=True, nullable=True)
    vendor_code: Mapped[str] = mapped_column(String(160), default="")
    product_name: Mapped[str] = mapped_column(String(500), default="")
    sold_qty: Mapped[int] = mapped_column(Integer, default=0)
    sales_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    before_spp: Mapped[float | None] = mapped_column(Float, nullable=True)
    spp_amount: Mapped[float | None] = mapped_column(Float, nullable=True)
    after_spp: Mapped[float | None] = mapped_column(Float, nullable=True)
    to_pay: Mapped[float | None] = mapped_column(Float, nullable=True)
    cost_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    total_cost_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    commission: Mapped[float | None] = mapped_column(Float, nullable=True)
    logistics: Mapped[float | None] = mapped_column(Float, nullable=True)
    storage: Mapped[float | None] = mapped_column(Float, nullable=True)
    returns: Mapped[float | None] = mapped_column(Float, nullable=True)
    acquiring: Mapped[float | None] = mapped_column(Float, nullable=True)
    spa: Mapped[float | None] = mapped_column(Float, nullable=True)
    advertising: Mapped[float | None] = mapped_column(Float, nullable=True)
    tax: Mapped[float | None] = mapped_column(Float, nullable=True)
    penalties: Mapped[float | None] = mapped_column(Float, nullable=True)
    deductions: Mapped[float | None] = mapped_column(Float, nullable=True)
    other_expenses: Mapped[float | None] = mapped_column(Float, nullable=True)
    profit: Mapped[float | None] = mapped_column(Float, nullable=True)
    profit_per_unit: Mapped[float | None] = mapped_column(Float, nullable=True)
    margin: Mapped[float | None] = mapped_column(Float, nullable=True)
    drr: Mapped[float | None] = mapped_column(Float, nullable=True)
    orders_qty: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(80), default="Нет данных")
    action: Mapped[str] = mapped_column(String(160), default="Проверить данные")


class SyncJob(Base):
    __tablename__ = "sync_jobs"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    wb_token_id: Mapped[int | None] = mapped_column(ForeignKey("wb_tokens.id"), nullable=True)
    status: Mapped[str] = mapped_column(String(40), default="queued")
    sync_type: Mapped[str] = mapped_column(String(40), default="manual_sync")
    progress_percent: Mapped[int] = mapped_column(Integer, default=0)
    current_step: Mapped[str | None] = mapped_column(String(80), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    steps: Mapped[list["SyncStep"]] = relationship(back_populates="sync_job", cascade="all, delete-orphan")


class SyncStep(Base):
    __tablename__ = "sync_steps"

    id: Mapped[int] = mapped_column(primary_key=True)
    sync_job_id: Mapped[int] = mapped_column(ForeignKey("sync_jobs.id"), index=True)
    step_name: Mapped[str] = mapped_column(String(80))
    status: Mapped[str] = mapped_column(String(40), default="pending")
    records_received: Mapped[int | None] = mapped_column(Integer, nullable=True)
    records_saved: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    sync_job: Mapped["SyncJob"] = relationship(back_populates="steps")


class ProductCostHistory(Base):
    __tablename__ = "product_cost_history"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    nm_id: Mapped[int] = mapped_column(Integer, index=True)
    vendor_code: Mapped[str] = mapped_column(String(120), default="")
    old_cost_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    new_cost_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    changed_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class Subscription(Base):
    __tablename__ = "subscriptions"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), unique=True, index=True)
    status: Mapped[str] = mapped_column(String(40), default=SubscriptionStatus.trial.value)
    trial_end: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    paid_until: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    user: Mapped["User"] = relationship(back_populates="subscription")


class SubscriptionHistory(Base):
    __tablename__ = "subscription_history"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    admin_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), index=True, nullable=True)
    previous_status: Mapped[str | None] = mapped_column(String(40), nullable=True)
    new_status: Mapped[str] = mapped_column(String(40))
    days_added: Mapped[int | None] = mapped_column(Integer, nullable=True)
    previous_end_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    new_end_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class PasswordResetToken(Base):
    __tablename__ = "password_reset_tokens"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    token: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime)
    used: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
