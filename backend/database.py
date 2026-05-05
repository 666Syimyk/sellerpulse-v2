from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from config import get_settings


settings = get_settings()
database_url = settings.sqlalchemy_database_url
connect_args = {"check_same_thread": False, "timeout": 30} if database_url.startswith("sqlite") else {}
engine = create_engine(database_url, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


if database_url.startswith("sqlite"):
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.execute("PRAGMA synchronous=NORMAL")
            try:
                cursor.execute("PRAGMA journal_mode=WAL")
            except Exception:
                # If DB is already locked, keep the connection usable with default journal mode.
                pass
        finally:
            cursor.close()


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def run_sqlite_migrations() -> None:
    # PostgreSQL: safely add any missing columns
    if not database_url.startswith("sqlite"):
        with engine.begin() as connection:
            for col_def in ["tax_rate FLOAT"]:
                try:
                    connection.execute(text(f"ALTER TABLE products ADD COLUMN IF NOT EXISTS {col_def}"))
                except Exception:
                    pass
        return

    with engine.begin() as connection:
        inspector = inspect(connection)
        tables = set(inspector.get_table_names())

        def add_column(table: str, column_sql: str) -> None:
            column_name = column_sql.split()[0]
            columns = {column["name"] for column in inspector.get_columns(table)}
            if column_name not in columns:
                connection.execute(text(f"ALTER TABLE {table} ADD COLUMN {column_sql}"))

        if "users" in tables:
            add_column("users", "is_admin BOOLEAN NOT NULL DEFAULT 0")

        if "wb_tokens" in tables:
            add_column("wb_tokens", "is_active BOOLEAN NOT NULL DEFAULT 1")
            add_column("wb_tokens", "sync_in_progress BOOLEAN NOT NULL DEFAULT 0")
            add_column("wb_tokens", "last_checked_at DATETIME")
            add_column("wb_tokens", "updated_at DATETIME")
            connection.execute(text("UPDATE wb_tokens SET is_active = 0, sync_in_progress = 0 WHERE token_status = 'invalid'"))
            connection.execute(
                text(
                    """
                    UPDATE wb_tokens
                    SET is_active = 0
                    WHERE is_active = 1
                      AND token_status != 'invalid'
                      AND id NOT IN (
                          SELECT MAX(id)
                          FROM wb_tokens
                          WHERE is_active = 1 AND token_status != 'invalid'
                          GROUP BY user_id
                      )
                    """
                )
            )

        for table in ("products", "sales", "orders", "expenses", "stocks", "dashboard_cache"):
            if table in tables:
                add_column(table, "wb_token_id INTEGER")

        if "sales" in tables:
            add_column("sales", "to_pay FLOAT")

        if "orders" in tables:
            add_column("orders", "total_before_spp FLOAT")
            add_column("orders", "spp_amount FLOAT")
            add_column("orders", "total_after_spp FLOAT")

        if "products" in tables:
            add_column("products", "brand VARCHAR(255)")
            add_column("products", "category VARCHAR(255)")
            add_column("products", "tax_rate FLOAT")

        if "expenses" in tables:
            add_column("expenses", "returns_qty INTEGER")

        if "financial_report_items" in tables:
            add_column("financial_report_items", "to_pay FLOAT")

        if "financial_reports" in tables:
            add_column("financial_reports", "manual_tax FLOAT")
            add_column("financial_reports", "manual_advertising FLOAT")
            add_column("financial_reports", "manual_other_expenses FLOAT")
            add_column("financial_reports", "validation_json JSON")
            add_column("financial_reports", "source_rows_json JSON")

        if "financial_report_items" in tables:
            add_column("financial_report_items", "orders_qty INTEGER")

        if "wb_raw_responses" not in tables:
            connection.execute(
                text(
                    """
                    CREATE TABLE wb_raw_responses (
                        id INTEGER PRIMARY KEY,
                        user_id INTEGER,
                        wb_token_id INTEGER,
                        endpoint VARCHAR(500) NOT NULL,
                        request_params_json JSON,
                        response_json JSON,
                        status_code INTEGER,
                        error_message TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )

        if "product_cost_history" not in tables:
            connection.execute(
                text(
                    """
                    CREATE TABLE product_cost_history (
                        id INTEGER PRIMARY KEY,
                        user_id INTEGER NOT NULL,
                        nm_id INTEGER NOT NULL,
                        vendor_code VARCHAR(120) DEFAULT '',
                        old_cost_price FLOAT,
                        new_cost_price FLOAT,
                        changed_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            connection.execute(text("CREATE INDEX IF NOT EXISTS ix_product_cost_history_user_id ON product_cost_history (user_id)"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS ix_product_cost_history_nm_id ON product_cost_history (nm_id)"))

        if "sync_jobs" not in tables:
            connection.execute(text("""
                CREATE TABLE sync_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    wb_token_id INTEGER REFERENCES wb_tokens(id),
                    status VARCHAR(40) NOT NULL DEFAULT 'queued',
                    sync_type VARCHAR(40) NOT NULL DEFAULT 'manual_sync',
                    progress_percent INTEGER NOT NULL DEFAULT 0,
                    current_step VARCHAR(80),
                    started_at DATETIME,
                    finished_at DATETIME,
                    last_error VARCHAR(1024),
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """))
            connection.execute(text("CREATE INDEX IF NOT EXISTS ix_sync_jobs_user_id ON sync_jobs (user_id)"))

        if "sync_steps" not in tables:
            connection.execute(text("""
                CREATE TABLE sync_steps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sync_job_id INTEGER NOT NULL REFERENCES sync_jobs(id),
                    step_name VARCHAR(80) NOT NULL,
                    status VARCHAR(40) NOT NULL DEFAULT 'pending',
                    records_received INTEGER,
                    records_saved INTEGER,
                    error_message VARCHAR(1024),
                    started_at DATETIME,
                    finished_at DATETIME
                )
            """))
            connection.execute(text("CREATE INDEX IF NOT EXISTS ix_sync_steps_job_id ON sync_steps (sync_job_id)"))

        if "subscription_history" not in tables:
            connection.execute(text("""
                CREATE TABLE subscription_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    admin_user_id INTEGER REFERENCES users(id),
                    previous_status VARCHAR(40),
                    new_status VARCHAR(40) NOT NULL,
                    days_added INTEGER,
                    previous_end_at DATETIME,
                    new_end_at DATETIME,
                    notes TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """))
            connection.execute(text("CREATE INDEX IF NOT EXISTS ix_subscription_history_user_id ON subscription_history (user_id)"))
            connection.execute(text("CREATE INDEX IF NOT EXISTS ix_subscription_history_admin_user_id ON subscription_history (admin_user_id)"))

        if "products" in tables:
            pragma = connection.execute(text("PRAGMA table_info(products)")).fetchall()
            col_info = {row[1]: row for row in pragma}
            wb_token_col = col_info.get("wb_token_id")
            wb_token_notnull = wb_token_col[3] if wb_token_col else 0
            if wb_token_notnull:
                connection.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS products_new (
                            id INTEGER PRIMARY KEY,
                            user_id INTEGER NOT NULL,
                            wb_token_id INTEGER,
                            nm_id INTEGER NOT NULL,
                            vendor_code VARCHAR(120) DEFAULT '',
                            name VARCHAR(500) DEFAULT '',
                            brand VARCHAR(255),
                            category VARCHAR(255),
                            cost_price FLOAT,
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    )
                )
                connection.execute(text(
                    "INSERT OR IGNORE INTO products_new "
                    "(id, user_id, wb_token_id, nm_id, vendor_code, name, brand, category, cost_price, created_at, updated_at) "
                    "SELECT id, user_id, wb_token_id, nm_id, vendor_code, name, brand, category, cost_price, created_at, updated_at "
                    "FROM products"
                ))
                connection.execute(text("DROP TABLE products"))
                connection.execute(text("ALTER TABLE products_new RENAME TO products"))
                connection.execute(text("CREATE INDEX IF NOT EXISTS ix_products_nm_id ON products (nm_id)"))
                connection.execute(text("CREATE INDEX IF NOT EXISTS ix_products_user_id ON products (user_id)"))
                connection.execute(text("CREATE INDEX IF NOT EXISTS ix_products_wb_token_id ON products (wb_token_id)"))
                connection.execute(text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_products_token_nm ON products (wb_token_id, nm_id) "
                    "WHERE wb_token_id IS NOT NULL"
                ))

        # Migrate products unique constraint from (wb_token_id, nm_id) to (user_id, nm_id)
        if "products" in tables:
            existing_indexes = {idx["name"] for idx in inspector.get_indexes("products")}
            if "uq_products_user_nm" not in existing_indexes:
                # Deduplicate: keep row with cost_price, else latest id, per (user_id, nm_id)
                connection.execute(text("""
                    DELETE FROM products WHERE id NOT IN (
                        SELECT MAX(CASE WHEN cost_price IS NOT NULL THEN id ELSE 0 END) AS keep_id
                        FROM products GROUP BY user_id, nm_id
                        HAVING MAX(CASE WHEN cost_price IS NOT NULL THEN id ELSE 0 END) > 0
                        UNION
                        SELECT MAX(id) FROM products
                        GROUP BY user_id, nm_id
                        HAVING MAX(CASE WHEN cost_price IS NOT NULL THEN 1 ELSE 0 END) = 0
                    )
                """))
                connection.execute(text("DROP INDEX IF EXISTS uq_products_token_nm"))
                connection.execute(text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS uq_products_user_nm ON products (user_id, nm_id)"
                ))
