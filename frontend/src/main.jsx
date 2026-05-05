import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  Activity,
  AlertTriangle,
  BadgePercent,
  BarChart3,
  Bell,
  Boxes,
  BriefcaseBusiness,
  CheckCircle2,
  ChevronDown,
  CircleDollarSign,
  Coins,
  Database,
  Download,
  FileSpreadsheet,
  Gauge,
  KeyRound,
  LineChart,
  LockKeyhole,
  LogOut,
  Mail,
  PackageCheck,
  RefreshCcw,
  Save,
  Search,
  ShieldCheck,
  ShoppingCart,
  Sparkles,
  Store,
  Trash2,
  TrendingUp,
  Undo2,
  UploadCloud,
  UserRound,
  WalletCards,
} from "lucide-react";
import { API_URL, api, getToken, setToken, subscriptionExpiredHandlers } from "./api/client";
import { dateTime, money, number, percent } from "./utils/format";
import "./styles.css";

const PERIODS = [
  ["today", "Сегодня"],
  ["week", "Неделя"],
  ["month", "Месяц"],
  ["last_month", "Прошлый месяц"],
  ["report", "По отчёту"],
];

const EXPENSE_LABELS = {
  commission: "Комиссия WB",
  logistics: "Логистика",
  storage: "Хранение",
  returns: "Возвраты",
  acquiring: "Эквайринг",
  spa: "СПА",
  advertising: "Реклама",
  tax: "Налог",
  cost_price: "Себестоимость",
  penalties: "Штрафы",
  deductions: "Удержания",
  other_expenses: "Прочие расходы",
};

const SOURCE_LABELS = {
  products: "Товары",
  sales: "Продажи",
  orders: "Заказы",
  stocks: "Остатки",
  finance: "Финансы",
  advertising: "Реклама",
};

const SOURCE_STATUS_TEXT = {
  ok: "получено",
  rate_limited: "лимит WB",
  limited: "нет прав",
  api_error: "ошибка WB",
};

const REQUIRED_RIGHTS = [
  "Контент",
  "Статистика",
  "Аналитика",
  "Финансы",
  "Продвижение",
  "Цены и скидки",
  "Поставки",
  "Возвраты",
];

const STATUS_META = {
  active: { label: "active", text: "Токен активен", className: "active" },
  limited: { label: "limited", text: "Не хватает прав", className: "limited" },
  invalid: { label: "invalid", text: "Токен неверный", className: "invalid" },
  rate_limited: { label: "rate_limited", text: "Лимит WB", className: "rate_limited" },
  api_error: { label: "api_error", text: "Ошибка WB API", className: "api_error" },
};

function App() {
  const [session, setSession] = useState({ loading: true, user: null });
  const [screen, setScreen] = useState("landing");

  useEffect(() => {
    if (!getToken()) {
      setSession({ loading: false, user: null });
      setScreen("landing");
      return;
    }
    api("/auth/me")
      .then((user) => {
        setSession({ loading: false, user });
        afterAuth(user);
      })
      .catch(() => {
        setToken(null);
        setSession({ loading: false, user: null });
        setScreen("landing");
      });
  }, []);

  useEffect(() => {
    const handler = () => setScreen("subscription");
    subscriptionExpiredHandlers.add(handler);
    return () => subscriptionExpiredHandlers.delete(handler);
  }, []);

  function afterAuth(user) {
    if (user.is_admin) {
      setScreen("admin");
    } else if (!user.subscription?.active) {
      setScreen("subscription");
    } else if (!user.has_wb_token) {
      setScreen("token");
    } else {
      setScreen("dashboard");
    }
  }

  function onAuth(user) {
    setSession({ loading: false, user });
    afterAuth(user);
  }

  function logout() {
    setToken(null);
    setSession({ loading: false, user: null });
    setScreen("landing");
  }

  if (session.loading) return <div className="boot"><Activity size={28} /> SellerPulse</div>;
  if (screen === "landing") return <LandingPage onLogin={() => setScreen("auth")} />;
  if (screen === "auth" || !session.user) return <AuthPage onAuth={onAuth} onBack={() => setScreen("landing")} />;
  if (screen === "subscription") return <SubscriptionPage user={session.user} onLogout={logout} onActivated={() => afterAuth(session.user)} />;
  if (screen === "admin") return <AdminPage user={session.user} onLogout={logout} onNavigate={setScreen} />;
  if (screen === "token") return <TokenPage user={session.user} onConnected={() => setScreen("dashboard")} onLogout={logout} onNavigate={setScreen} />;
  if (screen === "costs") return <CostPricePage user={session.user} onLogout={logout} onNavigate={setScreen} />;
  if (screen === "financial-report") return <FinancialReportPage user={session.user} onLogout={logout} onNavigate={setScreen} />;
  return <Dashboard user={session.user} onLogout={logout} onNavigate={setScreen} />;
}

function AuthPage({ onAuth, onBack }) {
  const [mode, setMode] = useState("login");
  const [form, setForm] = useState({ name: "", email: "", password: "" });
  const [resetForm, setResetForm] = useState({ email: "", token: "", new_password: "" });
  const [resetStep, setResetStep] = useState(1);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  async function submit(event) {
    event.preventDefault();
    setError(""); setMessage("");
    try {
      const result = await api(`/auth/${mode}`, { method: "POST", body: JSON.stringify(form) });
      setToken(result.access_token);
      const user = await api("/auth/me");
      onAuth(user);
    } catch (err) {
      setError(err.message);
    }
  }

  async function submitForgot(event) {
    event.preventDefault();
    setError(""); setMessage("");
    try {
      const result = await api("/auth/forgot-password", { method: "POST", body: JSON.stringify({ email: resetForm.email }) });
      setMessage(result.message);
      if (result.reset_token) {
        setResetForm((f) => ({ ...f, token: result.reset_token }));
      }
      setResetStep(2);
    } catch (err) { setError(err.message); }
  }

  async function submitReset(event) {
    event.preventDefault();
    setError(""); setMessage("");
    try {
      const result = await api("/auth/reset-password", { method: "POST", body: JSON.stringify({ token: resetForm.token, new_password: resetForm.new_password }) });
      setMessage(result.message);
      setMode("login");
      setResetStep(1);
    } catch (err) { setError(err.message); }
  }

  const isForgot = mode === "forgot";

  return (
    <main className="auth-page">
      <section className="auth-brand-panel">
        <BrandMark />
        <div className="auth-copy">
          <span className="soft-label"><Sparkles size={16} /> SellerPulse v2</span>
          <h1>Аналитика Wildberries без ручных таблиц</h1>
          <p>Подключите WB API-токен, укажите себестоимость и смотрите продажи, расходы, прибыль и действия на сегодня в одном аккуратном Dashboard.</p>
        </div>
        <div className="auth-benefits">
          <Benefit icon={LineChart} title="Контроль продаж" text="Периоды, выручка, маржа и ДРР." />
          <Benefit icon={Coins} title="Авторасходы WB" text="Комиссия, логистика, хранение и реклама." />
          <Benefit icon={ShieldCheck} title="Точная прибыль" text="Себестоимость плюс финансовые отчёты WB." />
        </div>
        <div className="auth-illustration" aria-hidden="true">
          <div className="chart-card"><span /><span /><span /><strong>+21%</strong></div>
        </div>
      </section>

      <section className="auth-card">
        <div className="auth-card-inner">
          {isForgot ? (
            <>
              <div className="auth-heading">
                <h2>{resetStep === 1 ? "Сброс пароля" : "Новый пароль"}</h2>
                <p>{resetStep === 1 ? "Введите email — вышлем ссылку для сброса." : "Введите код из письма и новый пароль."}</p>
              </div>
              {resetStep === 1 ? (
                <form onSubmit={submitForgot} className="form-stack">
                  <Field icon={Mail} label="E-mail" type="email" required value={resetForm.email} onChange={(v) => setResetForm((f) => ({ ...f, email: v }))} />
                  {error && <div className="error">{error}</div>}
                  {message && <div className="notice info compact">{message}</div>}
                  <button className="primary wide">Отправить ссылку</button>
                  <button type="button" onClick={() => { setMode("login"); setError(""); setMessage(""); }}>← Вернуться ко входу</button>
                </form>
              ) : (
                <form onSubmit={submitReset} className="form-stack">
                  <Field icon={KeyRound} label="Код из письма" value={resetForm.token} onChange={(v) => setResetForm((f) => ({ ...f, token: v }))} />
                  <Field icon={LockKeyhole} label="Новый пароль" type="password" required value={resetForm.new_password} onChange={(v) => setResetForm((f) => ({ ...f, new_password: v }))} />
                  {error && <div className="error">{error}</div>}
                  {message && <div className="notice info compact">{message}</div>}
                  <button className="primary wide">Сменить пароль</button>
                  <button type="button" onClick={() => { setResetStep(1); setError(""); setMessage(""); }}>← Назад</button>
                </form>
              )}
            </>
          ) : (
            <>
              <div className="auth-tabs">
                <button type="button" className={mode === "login" ? "selected" : ""} onClick={() => { setMode("login"); setError(""); }}>Вход</button>
                <button type="button" className={mode === "register" ? "selected" : ""} onClick={() => { setMode("register"); setError(""); }}>Регистрация</button>
              </div>
              <div className="auth-heading">
                <h2>{mode === "login" ? "Добро пожаловать" : "Создать аккаунт"}</h2>
                <p>{mode === "login" ? "Войдите, чтобы открыть кабинет SellerPulse." : "14 дней бесплатно — без карты."}</p>
              </div>
              <form onSubmit={submit} className="form-stack">
                {mode === "register" && <Field icon={UserRound} label="Имя" value={form.name} onChange={(v) => setForm({ ...form, name: v })} />}
                <Field icon={Mail} label="E-mail" type="email" required value={form.email} onChange={(v) => setForm({ ...form, email: v })} />
                <Field icon={LockKeyhole} label="Пароль" type="password" required value={form.password} onChange={(v) => setForm({ ...form, password: v })} />
                {error && <div className="error">{error}</div>}
                <button className="primary wide">{mode === "login" ? "Войти" : "Начать бесплатно"}</button>
                {mode === "login" && (
                  <button type="button" className="auth-link-btn" onClick={() => { setMode("forgot"); setError(""); setMessage(""); }}>Забыли пароль?</button>
                )}
              </form>
            </>
          )}
          {onBack && (
            <button type="button" className="auth-back-btn" onClick={onBack}>← На главную</button>
          )}
        </div>
      </section>
    </main>
  );
}

function TokenPage({ user, onConnected, onLogout, onNavigate }) {
  const [token, setTokenValue] = useState("");
  const [status, setStatus] = useState(null);
  const [dashboard, setDashboard] = useState(null);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [checking, setChecking] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const textareaRef = useRef(null);

  useEffect(() => {
    refreshStatus().catch(console.error);
  }, []);

  async function refreshStatus() {
    const tokenResult = await api("/wb-token").catch(() => null);
    const dashboardResult = await api("/dashboard?period=month").catch(() => null);
    setStatus(tokenResult);
    setDashboard(dashboardResult);
  }

  async function connectToken(event) {
    event?.preventDefault();
    setError("");
    setMessage("");
    if (!token.trim()) {
      setError("Вставьте WB API-токен.");
      return;
    }
    setChecking(true);
    try {
      const result = await api("/wb-token", { method: "POST", body: JSON.stringify({ token }) });
      setStatus(result);
      setMessage(result.message || statusText(result.status));
      if (result.connected && result.status !== "invalid") onConnected();
    } catch (err) {
      setError(err.message);
    } finally {
      setChecking(false);
    }
  }

  async function checkCurrentToken() {
    if (!status?.connected) return;
    setChecking(true);
    setError("");
    setMessage("");
    try {
      const result = await api("/wb-token/check", { method: "POST" });
      setStatus(result);
      setMessage(result.message || statusText(result.status));
      if (!result.connected) setDashboard(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setChecking(false);
    }
  }

  async function syncData() {
    setSyncing(true);
    setError("");
    setMessage("");
    try {
      const result = await api("/dashboard/sync?period=month", { method: "POST" });
      setDashboard(result.dashboard);
      setMessage(result.sync?.message || "Синхронизация завершена.");
      await refreshStatus();
    } catch (err) {
      setError(err.message);
    } finally {
      setSyncing(false);
    }
  }

  async function deleteToken() {
    if (!status?.connected) return;
    setDeleting(true);
    setError("");
    setMessage("");
    try {
      const result = await api("/wb-token", { method: "DELETE" });
      setStatus(result);
      setDashboard(null);
      setTokenValue("");
      setMessage(result.message || "WB API-токен удалён.");
    } catch (err) {
      setError(err.message);
    } finally {
      setDeleting(false);
    }
  }

  const shop = dashboard?.shop || {
    name: status?.shop_name,
    token_status: status?.status,
    last_sync_at: null,
  };

  return (
    <AppShell user={user} active="token" onLogout={onLogout} onNavigate={onNavigate} shop={shop} onSync={status?.connected ? syncData : null} syncing={syncing}>
      <section className="page-title">
        <div>
          <p className="eyebrow">Шаг 2 из 3</p>
          <h1>Подключение WB API-токена</h1>
          <p>Вставьте токен из кабинета Wildberries. SellerPulse проверит доступы и сохранит подключение.</p>
        </div>
      </section>

      <section className="token-layout">
        <form className="panel token-card" onSubmit={connectToken}>
          <div className="panel-head">
            <div>
              <h2>Ваш токен Wildberries</h2>
              <p>Можно вставить токен целиком, с префиксом Bearer или без него.</p>
            </div>
            <KeyRound size={22} />
          </div>
          <textarea
            ref={textareaRef}
            required
            placeholder="Вставьте API-токен Wildberries здесь..."
            value={token}
            onChange={(event) => setTokenValue(event.target.value)}
          />
          {error && <div className="error">{error}</div>}
          {message && <div className="notice info compact">{message}</div>}
          <div className="actions">
            <button className="primary" disabled={checking}><ShieldCheck size={18} /> {checking ? "Проверяем" : "Проверить и подключить"}</button>
            <button type="button" onClick={checkCurrentToken} disabled={!status?.connected || checking}><RefreshCcw size={17} /> Проверить текущий токен</button>
          </div>
          <div className="token-management">
            <h3>Управление токеном</h3>
            <div className="actions">
              <button type="button" onClick={() => { setTokenValue(""); textareaRef.current?.focus(); }}><KeyRound size={17} /> Заменить токен</button>
              <button type="button" className="danger" onClick={deleteToken} disabled={!status?.connected || deleting}><Trash2 size={17} /> {deleting ? "Удаляем" : "Удалить токен"}</button>
              <button type="button" onClick={syncData} disabled={!status?.connected || syncing}><RefreshCcw size={17} /> Синхронизировать данные</button>
            </div>
          </div>
        </form>

        <aside className="panel status-card">
          <div className="panel-head">
            <div>
              <h2>Статус токена</h2>
              <p>{statusText(status?.status)}</p>
            </div>
            <StatusPill status={status?.status || "invalid"} />
          </div>
          <div className="status-list">
            <InfoLine label="WB кабинет" value={status?.shop_name || shop?.name || "Не подключён"} />
            <InfoLine label="Последняя проверка" value={dateTime(status?.last_checked_at)} />
            <InfoLine label="Последняя синхронизация" value={dateTime(shop?.last_sync_at)} />
          </div>
          <h3>Права токена</h3>
          <RightsList permissions={status?.permissions} />
        </aside>
      </section>
    </AppShell>
  );
}

function Dashboard({ user, onLogout, onNavigate }) {
  const [period, setPeriod] = useState("month");
  const [data, setData] = useState(null);
  const [syncStatus, setSyncStatus] = useState(null);
  const [syncMessage, setSyncMessage] = useState("");
  const [error, setError] = useState("");
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [sortMode, setSortMode] = useState("attention");
  const [financialSortMode, setFinancialSortMode] = useState("default");
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const syncPollRef = useRef(null);
  const dashboardReloadedRef = useRef(false);
  const autoRestartedRef = useRef(false);

  async function load(nextPeriod = period) {
    setLoading(true);
    setError("");
    try {
      const result = await api(`/dashboard?period=${nextPeriod}`);
      setData(result);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  async function loadSyncStatus() {
    try {
      const result = await api("/sync/status");
      setSyncStatus(result);
      const active = result.status === "queued" || result.status === "running" || result.status === "partial";
      if (active) {
        dashboardReloadedRef.current = false;
        syncPollRef.current = setTimeout(loadSyncStatus, 3000);
      } else if (result.status === "completed" && !dashboardReloadedRef.current) {
        dashboardReloadedRef.current = true;
        load(period).catch(console.error);
      } else if (
        result.status === "failed" &&
        result.last_error?.includes("прервана") &&
        !autoRestartedRef.current
      ) {
        autoRestartedRef.current = true;
        try {
          await api(`/dashboard/sync?period=${period}`, { method: "POST" });
          syncPollRef.current = setTimeout(loadSyncStatus, 2000);
        } catch (_e) { /* silent */ }
      }
    } catch (_err) {
      // Silent — polling errors shouldn't block the UI
    }
  }

  useEffect(() => {
    load(period).catch(console.error);
  }, [period]);

  useEffect(() => {
    loadSyncStatus();
    return () => clearTimeout(syncPollRef.current);
  }, []);

  async function exportExcel() {
    try {
      const token = getToken();
      const response = await fetch(`${API_URL}/dashboard/export?period=${period}`, {
        headers: token ? { Authorization: `Bearer ${token}` } : {},
      });
      if (!response.ok) throw new Error("Ошибка экспорта");
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `sellerpulse_${period}.xlsx`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      setError(err.message);
    }
  }

  async function sync() {
    setSyncing(true);
    setError("");
    clearTimeout(syncPollRef.current);
    try {
      const result = await api(`/dashboard/sync?period=${period}`, { method: "POST" });
      setSyncMessage(result.message || "");
      if (result.sync_status) setSyncStatus(result.sync_status);
      // Start polling for progress
      syncPollRef.current = setTimeout(loadSyncStatus, 3000);
    } catch (err) {
      setError(err.message);
    } finally {
      setSyncing(false);
    }
  }

  const isFinancialReport = data?.data_source?.type === "financial_report";
  const metrics = data?.metrics || {};
  const products = data?.products || [];
  const statuses = useMemo(() => [...new Set(products.map((row) => row.status).filter(Boolean))], [products]);

  const avgProfitPerUnit = useMemo(() => {
    if (!isFinancialReport) return null;
    const soldRows = products.filter((r) => (r.sold_qty || 0) > 0 && r.profit != null);
    const totalQty = soldRows.reduce((s, r) => s + (r.sold_qty || 0), 0);
    const totalProfit = soldRows.reduce((s, r) => s + r.profit, 0);
    return totalQty > 0 ? totalProfit / totalQty : null;
  }, [isFinancialReport, products]);

  const filteredProducts = useMemo(() => {
    const search = query.trim().toLowerCase();
    const filtered = [...products]
      .filter((row) => statusFilter === "all" || row.status === statusFilter)
      .filter((row) => {
        if (!search) return true;
        return [row.nm_id, row.vendor_code, row.name, row.brand, row.category, row.status, row.action]
          .some((value) => String(value || "").toLowerCase().includes(search));
      });

    if (isFinancialReport) {
      return filtered.sort((a, b) => {
        if (financialSortMode === "profit_desc") return nullableNumber(b.profit, Number.NEGATIVE_INFINITY) - nullableNumber(a.profit, Number.NEGATIVE_INFINITY);
        if (financialSortMode === "sales_desc") return nullableNumber(b.sold_qty, 0) - nullableNumber(a.sold_qty, 0);
        if (financialSortMode === "margin_desc") return nullableNumber(b.margin, Number.NEGATIVE_INFINITY) - nullableNumber(a.margin, Number.NEGATIVE_INFINITY);
        if (financialSortMode === "profit_per_unit_desc") return nullableNumber(b.profit_per_unit, Number.NEGATIVE_INFINITY) - nullableNumber(a.profit_per_unit, Number.NEGATIVE_INFINITY);
        // default: sold_qty > 0 first (by profit desc), then expenses-without-sales, then no-sales
        const aHasSales = (a.sold_qty || 0) > 0;
        const bHasSales = (b.sold_qty || 0) > 0;
        if (aHasSales && !bHasSales) return -1;
        if (!aHasSales && bHasSales) return 1;
        if (aHasSales && bHasSales) return nullableNumber(b.profit, Number.NEGATIVE_INFINITY) - nullableNumber(a.profit, Number.NEGATIVE_INFINITY);
        const aHasExp = a.status === "Расходы без продаж";
        const bHasExp = b.status === "Расходы без продаж";
        if (aHasExp && !bHasExp) return -1;
        if (!aHasExp && bHasExp) return 1;
        return 0;
      });
    }

    const statusRank = {
      "Нет себестоимости": 0,
      "В минусе": 1,
      "Минус с каждой продажи": 1,
      "Расходы без продаж": 1,
      "Низкая маржа": 2,
      "Заканчивается остаток": 3,
      "Нет данных WB": 4,
      "В плюсе": 5,
    };
    return filtered.sort((left, right) => {
      if (sortMode === "profit_asc") return nullableNumber(left.profit, Number.NEGATIVE_INFINITY) - nullableNumber(right.profit, Number.NEGATIVE_INFINITY);
      if (sortMode === "profit_desc") return nullableNumber(right.profit, Number.NEGATIVE_INFINITY) - nullableNumber(left.profit, Number.NEGATIVE_INFINITY);
      if (sortMode === "sales_desc") return nullableNumber(right.after_spp, 0) - nullableNumber(left.after_spp, 0);
      if (sortMode === "stock_asc") return nullableNumber(left.stock, Number.POSITIVE_INFINITY) - nullableNumber(right.stock, Number.POSITIVE_INFINITY);
      return (statusRank[left.status] ?? 10) - (statusRank[right.status] ?? 10);
    });
  }, [products, query, statusFilter, sortMode, financialSortMode, isFinancialReport]);

  return (
    <AppShell
      user={user}
      active="dashboard"
      onLogout={onLogout}
      onNavigate={onNavigate}
      shop={data?.shop}
      onSync={sync}
      syncing={syncing}
      searchValue={query}
      onSearchChange={setQuery}
      searchPlaceholder="Поиск по товарам, артикулам, nmID, статусам..."
    >
      <section className="hero-card">
        <div className="hero-copy">
          <p className="eyebrow">Dashboard</p>
          <h1>Добро пожаловать, {data?.shop?.name || "WB кабинет"}</h1>
          <p>Следите за продажами, расходами WB, чистой прибылью и товарами, которые требуют внимания сегодня.</p>
          <div className="hero-meta">
            <StatusPill status={data?.shop?.token_status || "invalid"} />
            <span>Последняя синхронизация: {dateTime(data?.shop?.last_sync_at)}</span>
          </div>
        </div>
        <div className="hero-widget">
          <div>
            <small>Период</small>
            <strong>{PERIODS.find(([id]) => id === period)?.[1] || period}</strong>
          </div>
          <div>
            <small>Источник</small>
            <strong>{data?.data_source?.type === "financial_report" ? "Фин. отчёт" : data?.data_source?.type === "wb_api" ? "WB API" : "—"}</strong>
          </div>
          <div>
            <small>Токен</small>
            <strong>{data?.shop?.token_status === "active" ? "Активен" : data?.shop?.token_status || "—"}</strong>
          </div>
          <div>
            <small>Синхронизация</small>
            <strong>{data?.shop?.last_sync_at ? dateTime(data.shop.last_sync_at).split(",")[0] : "—"}</strong>
          </div>
        </div>
      </section>

      <div className="dashboard-toolbar">
        <div className="segments">
          {PERIODS.map(([id, label]) => <button key={id} className={period === id ? "selected" : ""} onClick={() => setPeriod(id)}>{label}</button>)}
        </div>
        <div className="dashboard-filters">
          <button type="button" className="export-btn" onClick={exportExcel} title="Скачать Excel"><Download size={16} /> Excel</button>
          <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
            <option value="all">Все статусы</option>
            {statuses.map((status) => <option key={status} value={status}>{status}</option>)}
          </select>
          {isFinancialReport ? (
            <select value={financialSortMode} onChange={(event) => setFinancialSortMode(event.target.value)}>
              <option value="default">По продажам (по умолчанию)</option>
              <option value="profit_desc">По прибыли</option>
              <option value="sales_desc">По кол-ву продаж</option>
              <option value="margin_desc">По марже</option>
              <option value="profit_per_unit_desc">По прибыли за 1 шт.</option>
            </select>
          ) : (
            <select value={sortMode} onChange={(event) => setSortMode(event.target.value)}>
              <option value="attention">Сначала требуют внимания</option>
              <option value="profit_asc">Прибыль: сначала минус</option>
              <option value="profit_desc">Прибыль: сначала плюс</option>
              <option value="sales_desc">Выручка: сначала больше</option>
              <option value="stock_asc">Остаток: сначала меньше</option>
            </select>
          )}
        </div>
      </div>

      {data?.data_source && data.data_source.type !== "no_report_for_period" && (
        <div className={`notice ${isFinancialReport ? "info" : "warning"}`}>
          {isFinancialReport ? <CheckCircle2 size={18} /> : <AlertTriangle size={18} />}
          <div>
            <strong>{data.data_source.label}</strong>
            <span>
              {isFinancialReport
                ? `Точные данные из файла ${data.data_source.file_name || "WB"}.`
                : data.data_source.message || "Предварительные данные WB API."}
            </span>
          </div>
        </div>
      )}
      {data?.today_hint && <div className="notice warning"><AlertTriangle size={18} /> Данные за сегодня могут быть предварительными. Точные расходы WB появляются после формирования финансового отчёта.</div>}
      {data?.data_source?.type === "no_report_for_period" && (
        <div className="notice warning"><AlertTriangle size={18} /> {data.data_source.message}</div>
      )}
      {data?.data_source?.type === "wb_api" && data?.shop?.token_status === "rate_limited" && (
        <div className="notice warning"><AlertTriangle size={18} /> WB временно ограничил запросы. Товары загружены: {data?.products?.length || 0}. Продажи, остатки и расходы появятся после снятия лимита WB.</div>
      )}
      {data?.data_source?.type === "wb_api" && data?.shop?.token_status === "api_error" && (
        <div className="notice warning"><AlertTriangle size={18} /> WB API временно отвечает с ошибками. Показываем последние сохранённые данные, недоступные поля отмечены как "Нет данных WB".</div>
      )}
      {error && <div className="notice warning"><AlertTriangle size={18} /> {error}</div>}
      {syncMessage && <div className="notice info"><Activity size={16} /> {syncMessage}</div>}
      <SyncProgressBanner syncStatus={syncStatus} onRestart={sync} />

      {loading ? <div className="loader">Загружаем показатели</div> : !products.length ? (
        <DashboardEmptyState data={data} onNavigate={onNavigate} onSync={sync} syncing={syncing} />
      ) : isFinancialReport ? (
        <>
          <section className="metrics-grid">
            <Metric icon={ShoppingCart} label="Продано, шт." value={number(metrics.sold_qty)} trend="за период" />
            <Metric icon={CircleDollarSign} label="Сумма продаж" value={money(metrics.sales_sum)} trend="по отчёту WB" />
            <Metric icon={BadgePercent} label="К перечислению" value={money(metrics.to_pay)} trend="база прибыли" />
            <Metric icon={Coins} label="Себестоимость" value={money(data?.expenses?.cost_price)} trend="сумма закупки" />
            <Metric icon={TrendingUp} label="Чистая прибыль" value={money(metrics.net_profit)} trend="по отчёту WB" />
            <Metric icon={LineChart} label="Прибыль с 1 шт." value={money(avgProfitPerUnit)} trend="среднее по товарам" />
            <Metric icon={Gauge} label="Маржа" value={percent(metrics.margin)} trend="прибыль / выручка" />
          </section>
          <FinancialPulseTable
            rows={filteredProducts}
            allRows={products}
            totalRows={products.length}
            query={query}
            onQueryChange={setQuery}
            reload={() => load(period)}
          />
        </>
      ) : (
        <>
          <section className="metrics-grid">
            <Metric icon={ShoppingCart} label="Продажи, шт." value={number(metrics.sold_qty)} trend="за период" />
            <Metric icon={CircleDollarSign} label="Сумма продаж" value={money(metrics.sales_sum)} trend="после заказов" />
            <Metric icon={BadgePercent} label="К перечислению" value={money(metrics.to_pay ?? metrics.after_spp)} trend="база прибыли" />
            <Metric icon={TrendingUp} label="Чистая прибыль" value={money(metrics.net_profit)} trend="предварительно" />
            <Metric icon={Gauge} label="Маржа" value={percent(metrics.margin)} trend="прибыль / выручка" />
            <Metric icon={BarChart3} label="ДРР" value={percent(metrics.drr)} trend="реклама / выручка" />
            <Metric icon={Undo2} label="Возвраты" value={number(metrics.returns_qty)} trend="шт." />
            <Metric icon={PackageCheck} label="Процент выкупа" value={percent(metrics.buyout_percent)} trend="продажи / заказы" />
          </section>
          <Expenses expenses={data.expenses} revenue={metrics.after_spp} />
          <PulseTable
            rows={filteredProducts}
            totalRows={products.length}
            query={query}
            onQueryChange={setQuery}
            reload={() => load(period)}
          />
        </>
      )}
    </AppShell>
  );
}

function DashboardEmptyState({ data, onNavigate, onSync, syncing }) {
  const hasToken = data?.shop?.token_status && data.shop.token_status !== "invalid";
  const noReportForPeriod = data?.data_source?.type === "no_report_for_period";
  return (
    <section className="panel empty-dashboard">
      <div className="empty-dashboard-icon"><FileSpreadsheet size={30} /></div>
      <div>
        <p className="eyebrow">{noReportForPeriod ? "Отчёт не за этот период" : "Основной сценарий"}</p>
        <h2>{noReportForPeriod ? "Нет финансового отчёта за выбранный период" : "Загрузите финансовый отчёт WB"}</h2>
        <p>
          {noReportForPeriod
            ? `Загруженный файл «${data.data_source.file_name || "отчёт"}» не покрывает выбранный период. Загрузите отчёт WB за нужный период.`
            : hasToken
              ? "Токен подключён. Если WB API временно ограничил запросы, точная аналитика всё равно строится через Excel или ZIP финансового отчёта WB."
              : "Нет данных WB. Подключите токен или загрузите финансовый отчёт WB, чтобы построить точную таблицу."}
        </p>
        <div className="actions">
          <button type="button" className="primary" onClick={() => onNavigate("financial-report")}><UploadCloud size={17} /> Загрузить финансовый отчёт WB</button>
          {hasToken && !noReportForPeriod && <button type="button" onClick={onSync} disabled={syncing}><RefreshCcw size={17} /> {syncing ? "Проверяем WB API" : "Проверить WB API"}</button>}
        </div>
      </div>
    </section>
  );
}

function CostPricePage({ user, onLogout, onNavigate }) {
  const [products, setProducts] = useState([]);
  const [costs, setCosts] = useState({});
  const [saving, setSaving] = useState({});
  const [savingAll, setSavingAll] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [query, setQuery] = useState("");
  const [filterMode, setFilterMode] = useState("all");
  const [loading, setLoading] = useState(true);
  const [importFile, setImportFile] = useState(null);
  const [importing, setImporting] = useState(false);
  const importInputRef = useRef(null);

  async function load() {
    setLoading(true);
    setError("");
    try {
      const result = await api("/products");
      setProducts(result || []);
      setCosts(
        Object.fromEntries(
          (result || []).map((p) => [p.nm_id, p.cost_price === null || p.cost_price === undefined ? "" : String(p.cost_price)])
        )
      );
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { load().catch(console.error); }, []);

  async function save(product) {
    const parsed = parseCost(costs[product.nm_id]);
    if (!parsed.ok) { setError(parsed.message); return; }
    setSaving((c) => ({ ...c, [product.nm_id]: true }));
    setError("");
    setMessage("");
    try {
      await api(`/products/${product.nm_id}/cost-price`, {
        method: "PATCH",
        body: JSON.stringify({ cost_price: parsed.value, vendor_code: product.vendor_code, name: product.name }),
      });
      setMessage(`Себестоимость ${product.vendor_code || product.nm_id} сохранена.`);
      setProducts((prev) => prev.map((p) => p.nm_id === product.nm_id ? { ...p, cost_price: parsed.value } : p));
    } catch (err) {
      setError(err.message);
    } finally {
      setSaving((c) => ({ ...c, [product.nm_id]: false }));
    }
  }

  async function saveAll() {
    const changed = changedProducts;
    if (!changed.length) { setMessage("Нет изменений для сохранения."); return; }
    const invalid = changed.map((p) => ({ p, parsed: parseCost(costs[p.nm_id]) })).find((x) => !x.parsed.ok);
    if (invalid) { setError(`${invalid.p.vendor_code || invalid.p.nm_id}: ${invalid.parsed.message}`); return; }
    setSavingAll(true);
    setError("");
    setMessage("");
    setSaving((c) => ({ ...c, ...Object.fromEntries(changed.map((p) => [p.nm_id, true])) }));
    try {
      for (const product of changed) {
        const parsed = parseCost(costs[product.nm_id]);
        await api(`/products/${product.nm_id}/cost-price`, {
          method: "PATCH",
          body: JSON.stringify({ cost_price: parsed.value, vendor_code: product.vendor_code, name: product.name }),
        });
      }
      setMessage(`Сохранено товаров: ${changed.length}.`);
      await load();
    } catch (err) {
      setError(err.message);
    } finally {
      setSavingAll(false);
      setSaving((c) => ({ ...c, ...Object.fromEntries(changed.map((p) => [p.nm_id, false])) }));
    }
  }

  async function importCosts() {
    if (!importFile) return;
    setImporting(true);
    setError("");
    setMessage("");
    try {
      const formData = new FormData();
      formData.append("file", importFile);
      const result = await api("/products/cost-prices/import", { method: "POST", body: formData });
      setMessage(result.message || `Импорт завершён. Обновлено: ${result.updated}.`);
      setImportFile(null);
      if (importInputRef.current) importInputRef.current.value = "";
      await load();
    } catch (err) {
      setError(err.message);
    } finally {
      setImporting(false);
    }
  }

  const filteredProducts = useMemo(() => {
    const search = query.trim().toLowerCase();
    return products
      .filter((p) => {
        if (filterMode === "no_cost") return p.cost_price === null || p.cost_price === undefined;
        if (filterMode === "has_cost") return p.cost_price !== null && p.cost_price !== undefined;
        return true;
      })
      .filter((p) => {
        if (!search) return true;
        return [p.nm_id, p.vendor_code, p.name].some((v) => String(v || "").toLowerCase().includes(search));
      });
  }, [products, query, filterMode]);

  const changedProducts = useMemo(() => {
    return products.filter((p) => {
      const current = normalizeCostValue(p.cost_price);
      const next = normalizeCostInput(costs[p.nm_id]);
      return current !== next;
    });
  }, [products, costs]);

  const noCostCount = products.filter((p) => p.cost_price === null || p.cost_price === undefined).length;

  return (
    <AppShell
      user={user}
      active="costs"
      onLogout={onLogout}
      onNavigate={onNavigate}
      searchValue={query}
      onSearchChange={setQuery}
      searchPlaceholder="Поиск по nmID, артикулу или названию..."
    >
      <section className="page-title costs-title">
        <div>
          <p className="eyebrow">Управление</p>
          <h1>Себестоимость товаров</h1>
          <p>Заполните себестоимость один раз. SellerPulse будет использовать её во всех будущих отчётах автоматически.</p>
        </div>
        <div className="cost-title-actions">
          <button type="button" className="primary" onClick={saveAll} disabled={loading || savingAll || !changedProducts.length}>
            <Save size={17} /> {savingAll ? "Сохраняем..." : `Сохранить изменения${changedProducts.length ? ` (${changedProducts.length})` : ""}`}
          </button>
          <button type="button" onClick={load} disabled={loading}><RefreshCcw size={17} /> Обновить</button>
        </div>
      </section>

      {message && <div className="notice info">{message}</div>}
      {error && <div className="notice warning"><AlertTriangle size={18} /> {error}</div>}

      {/* Import panel */}
      <section className="panel cost-import-panel">
        <div className="cost-import-head">
          <div>
            <h3>Импорт себестоимости из Excel / CSV</h3>
            <p>Файл должен содержать колонки: <strong>nmID</strong> (или «Артикул») и <strong>Себестоимость</strong>.</p>
          </div>
          <div className="cost-import-actions">
            <label className="file-picker small">
              <input
                ref={importInputRef}
                type="file"
                accept=".xlsx,.xls,.csv"
                onChange={(e) => setImportFile(e.target.files?.[0] || null)}
              />
              Выбрать файл
            </label>
            {importFile && <span className="selected-file small">{importFile.name}</span>}
            <button type="button" className="primary" onClick={importCosts} disabled={!importFile || importing}>
              <Download size={15} /> {importing ? "Импортируем..." : "Импортировать"}
            </button>
          </div>
        </div>
      </section>

      <section className="panel cost-panel">
        <div className="cost-toolbar">
          <label className="search-box cost-search">
            <Search size={18} />
            <input placeholder="Поиск по nmID, артикулу или названию..." value={query} onChange={(e) => setQuery(e.target.value)} />
          </label>
          <div className="cost-filter-tabs">
            <button className={filterMode === "all" ? "active" : ""} onClick={() => setFilterMode("all")}>Все ({products.length})</button>
            <button className={filterMode === "no_cost" ? "active" : ""} onClick={() => setFilterMode("no_cost")}>
              Без себестоимости{noCostCount ? ` (${noCostCount})` : ""}
            </button>
            <button className={filterMode === "has_cost" ? "active" : ""} onClick={() => setFilterMode("has_cost")}>С себестоимостью</button>
          </div>
          <div className="cost-toolbar-meta">
            <span>{filteredProducts.length} товаров</span>
            {changedProducts.length > 0 && (
              <button type="button" className="primary small" onClick={saveAll} disabled={savingAll}>
                <Save size={14} /> Сохранить всё ({changedProducts.length})
              </button>
            )}
          </div>
        </div>

        {loading ? <div className="loader inline">Загружаем товары</div> : (
          <div className="table-wrap cost-table">
            <table>
              <thead>
                <tr>
                  <th>WB nmID</th>
                  <th>Артикул</th>
                  <th>Название товара</th>
                  <th>Текущая себестоимость</th>
                  <th>Новая себестоимость</th>
                  <th>Обновлено</th>
                  <th>Действие</th>
                </tr>
              </thead>
              <tbody>
                {filteredProducts.map((product) => {
                  const hasCost = product.cost_price !== null && product.cost_price !== undefined;
                  const isChanged = normalizeCostValue(product.cost_price) !== normalizeCostInput(costs[product.nm_id]);
                  return (
                    <tr key={product.nm_id} className={isChanged ? "row-changed" : ""}>
                      <td>{product.nm_id}</td>
                      <td>{product.vendor_code || "—"}</td>
                      <td className="name">{product.name || "—"}</td>
                      <td>
                        {hasCost
                          ? <span className="cost-value">{money(product.cost_price)}</span>
                          : <span className="no-cost-label">Нет себестоимости</span>}
                      </td>
                      <td>
                        <input
                          className="cost-input"
                          type="number"
                          min="0"
                          step="0.01"
                          placeholder={hasCost ? "Изменить..." : "Введите себестоимость"}
                          value={costs[product.nm_id] ?? ""}
                          onChange={(e) => setCosts((c) => ({ ...c, [product.nm_id]: e.target.value }))}
                        />
                      </td>
                      <td className="cost-date">{product.updated_at ? product.updated_at.slice(0, 10) : "—"}</td>
                      <td>
                        <button
                          type="button"
                          className={`small ${isChanged ? "primary" : ""}`}
                          onClick={() => save(product)}
                          disabled={saving[product.nm_id] || !isChanged}
                        >
                          <Save size={14} /> {saving[product.nm_id] ? "Сохраняем..." : "Сохранить"}
                        </button>
                      </td>
                    </tr>
                  );
                })}
                {!filteredProducts.length && (
                  <tr><td colSpan="7" className="empty">
                    {products.length ? "По запросу ничего не найдено." : "Загрузите финансовый отчёт WB или синхронизируйте токен — товары появятся здесь."}
                  </td></tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </AppShell>
  );
}

function RnpCostCell({ item, costs, setCosts, saving, onSave }) {
  const [editing, setEditing] = useState(item.cost_price === null || item.cost_price === undefined);
  const hasCost = item.cost_price !== null && item.cost_price !== undefined;

  React.useEffect(() => {
    if (item.cost_price !== null && item.cost_price !== undefined) {
      setEditing(false);
    }
  }, [item.cost_price]);

  if (!editing && hasCost) {
    return (
      <div className="rnp-cost-display">
        <span>{money(item.cost_price)}</span>
        <button type="button" className="small" onClick={() => setEditing(true)}>Изменить</button>
      </div>
    );
  }

  return (
    <div className="inline-cost">
      <input
        className="mini"
        type="number"
        min="0"
        step="0.01"
        value={costs[item.id] ?? ""}
        placeholder={hasCost ? "Изменить..." : "Введите себестоимость"}
        autoFocus={hasCost}
        onChange={(e) => setCosts((c) => ({ ...c, [item.id]: e.target.value }))}
        onKeyDown={(e) => { if (e.key === "Escape" && hasCost) setEditing(false); }}
      />
      <button
        type="button"
        className="small primary"
        onClick={async () => { await onSave(item); if (hasCost) setEditing(false); }}
        disabled={saving[item.id]}
      >
        <Save size={14} /> {saving[item.id] ? "..." : ""}
      </button>
      {hasCost && (
        <button type="button" className="small" onClick={() => setEditing(false)}>✕</button>
      )}
    </div>
  );
}

function FinancialReportPage({ user, onLogout, onNavigate }) {
  const [reportData, setReportData] = useState({ report: null, items: [], raw_rows: [], columns: [], message: "" });
  const [file, setFile] = useState(null);
  const [dragging, setDragging] = useState(false);
  const [loading, setLoading] = useState(true);
  const [uploading, setUploading] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [activeTab, setActiveTab] = useState("rnp");
  // RNP tab
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [sortProfit, setSortProfit] = useState("asc");
  const [costs, setCosts] = useState({});
  const [saving, setSaving] = useState({});
  const [reportTax, setReportTax] = useState("");
  const [savingTax, setSavingTax] = useState(false);
  const [settingsCost, setSettingsCost] = useState("");
  const [settingsAdv, setSettingsAdv] = useState("");
  const [settingsOther, setSettingsOther] = useState("");
  const [applyingSettings, setApplyingSettings] = useState(false);
  // Raw rows tab
  const [rawQuery, setRawQuery] = useState("");
  const [rawPage, setRawPage] = useState(1);
  const RAW_PAGE_SIZE = 50;
  // Accuracy tab
  const [accuracy, setAccuracy] = useState(null);
  const [validating, setValidating] = useState(false);

  async function load() {
    setLoading(true);
    try {
      const result = await api("/financial-report");
      setReportData(result);
      setAccuracy(result.validation || null);
      setCosts(Object.fromEntries((result.items || []).map((item) => [item.id, item.cost_price ?? ""])));
      setReportTax(normalizeCostValue(result.report?.tax_percent ?? result.report?.manual_tax));
      setSettingsAdv(normalizeCostValue(result.report?.manual_advertising));
      setSettingsOther(normalizeCostValue(result.report?.manual_other_expenses));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load().catch(console.error);
  }, []);

  function selectFile(nextFile) {
    setFile(nextFile || null);
    setError("");
    setMessage("");
    setAccuracy(null);
  }

  async function validateFile() {
    if (!file) {
      setError("Выберите Excel, CSV или ZIP-файл финансового отчёта WB.");
      return;
    }
    setValidating(true);
    setError("");
    setMessage("");
    try {
      const formData = new FormData();
      formData.append("file", file);
      const result = await api("/financial-report/validate", { method: "POST", body: formData });
      setAccuracy(result);
      setMessage("Проверка точности выполнена.");
      setActiveTab("accuracy");
    } catch (err) {
      setError(err.message);
    } finally {
      setValidating(false);
    }
  }

  async function upload() {
    if (!file) {
      setError("Выберите Excel, CSV или ZIP-файл финансового отчёта WB.");
      return;
    }
    setUploading(true);
    setError("");
    setMessage("");
    try {
      const formData = new FormData();
      formData.append("file", file);
      const result = await api("/financial-report/upload", { method: "POST", body: formData });
      setReportData(result);
      setAccuracy(result.validation || null);
      setCosts(Object.fromEntries((result.items || []).map((item) => [item.id, item.cost_price ?? ""])));
      setReportTax(normalizeCostValue(result.report?.tax_percent ?? result.report?.manual_tax));
      setSettingsAdv(normalizeCostValue(result.report?.manual_advertising));
      setSettingsOther(normalizeCostValue(result.report?.manual_other_expenses));
      setMessage(result.message || "Финансовый отчёт обработан.");
      setActiveTab("raw");
      setRawPage(1);
      setRawQuery("");
    } catch (err) {
      setError(err.message);
    } finally {
      setUploading(false);
    }
  }

  async function clearReport() {
    await api("/financial-report", { method: "DELETE" });
    setReportData({ report: null, items: [], raw_rows: [], columns: [], message: "" });
    setFile(null);
    setReportTax("");
    setSettingsCost("");
    setSettingsAdv("");
    setSettingsOther("");
    setAccuracy(null);
    setMessage("Отчёт очищен.");
  }

  async function saveCost(item) {
    const parsed = parseCost(costs[item.id]);
    if (!parsed.ok) {
      setError(parsed.message);
      return;
    }
    setSaving((current) => ({ ...current, [item.id]: true }));
    setError("");
    try {
      const result = await api(`/financial-report/${reportData.report.id}/items/${item.id}/cost-price`, {
        method: "PATCH",
        body: JSON.stringify({ cost_price: parsed.value }),
      });
      setReportData(result);
      setAccuracy(result.validation || accuracy);
      setCosts(Object.fromEntries((result.items || []).map((row) => [row.id, row.cost_price ?? ""])));
      setMessage(`Себестоимость товара ${item.nm_id || item.vendor_code} сохранена.`);
    } catch (err) {
      setError(err.message);
    } finally {
      setSaving((current) => ({ ...current, [item.id]: false }));
    }
  }

  async function saveReportTax() {
    if (!reportData.report?.id) return;
    const parsed = parseCost(reportTax);
    if (!parsed.ok) {
      setError(parsed.message);
      return;
    }
    setSavingTax(true);
    setError("");
    try {
      const result = await api(`/financial-report/${reportData.report.id}/tax`, {
        method: "PATCH",
        body: JSON.stringify({ tax: parsed.value }),
      });
      setReportData(result);
      setAccuracy(result.validation || accuracy);
      setCosts(Object.fromEntries((result.items || []).map((row) => [row.id, row.cost_price ?? ""])));
      setReportTax(normalizeCostValue(result.report?.tax_percent ?? result.report?.manual_tax));
      setMessage(parsed.value === null ? "Налог отчёта очищен." : "Налог применён ко всему отчёту.");
    } catch (err) {
      setError(err.message);
    } finally {
      setSavingTax(false);
    }
  }

  async function applySettings() {
    if (!reportData.report?.id) return;
    if (reportTax !== "") {
      const v = parseFloat(reportTax);
      if (isNaN(v) || v < 0 || v > 100) {
        setError("Налог должен быть от 0 до 100%.");
        return;
      }
    }
    setApplyingSettings(true);
    setError("");
    try {
      const payload = {};
      if (settingsCost !== "") { const v = parseFloat(settingsCost); if (!isNaN(v) && v >= 0) payload.global_cost_price = v; }
      if (reportTax !== "") { const v = parseFloat(reportTax); if (!isNaN(v) && v >= 0 && v <= 100) payload.tax_percent = v; }
      if (settingsAdv !== "") { const v = parseFloat(settingsAdv); if (!isNaN(v) && v >= 0) payload.global_advertising = v; }
      if (settingsOther !== "") { const v = parseFloat(settingsOther); if (!isNaN(v) && v >= 0) payload.global_other_expenses = v; }
      const result = await api(`/financial-report/${reportData.report.id}/apply-settings`, {
        method: "POST",
        body: JSON.stringify(payload),
      });
      setReportData(result);
      setAccuracy(result.validation || accuracy);
      setCosts(Object.fromEntries((result.items || []).map((row) => [row.id, row.cost_price ?? ""])));
      setReportTax(normalizeCostValue(result.report?.tax_percent ?? result.report?.manual_tax));
      setSettingsAdv(normalizeCostValue(result.report?.manual_advertising));
      setSettingsOther(normalizeCostValue(result.report?.manual_other_expenses));
      setSettingsCost("");
      setMessage("Настройки применены, отчёт пересчитан.");
    } catch (err) {
      setError(err.message);
    } finally {
      setApplyingSettings(false);
    }
  }

  function exportCsv() {
    const headers = [
      "Артикул", "Название товара", "WB nmID", "Продано", "Возвраты", "Заказано", "% выкупа", "До СПП", "СПП", "После СПП", "К перечислению",
      "Себестоимость", "Комиссия WB", "Логистика", "Хранение", "Эквайринг", "СПА", "Реклама",
      "Налог", "Штрафы", "Удержания", "Прочие расходы", "Прибыль", "Прибыль за штуку", "Маржа", "ДРР", "Статус", "Действие",
    ];
    const rows = filteredItems.map((item) => [
      item.vendor_code, item.product_name, item.nm_id, item.sold_qty, item.returns, item.orders_qty, item.buyout_rate,
      item.before_spp, item.spp_amount, item.after_spp, item.to_pay, item.total_cost_price,
      item.commission, item.logistics, item.storage, item.acquiring, item.spa,
      item.advertising, item.tax, item.penalties, item.deductions, item.other_expenses,
      item.profit, item.profit_per_unit, item.margin, item.drr, item.status, item.action,
    ]);
    const csv = [headers, ...rows]
      .map((row) => row.map((value) => `"${String(value ?? "").replaceAll('"', '""')}"`).join(";"))
      .join("\n");
    const blob = new Blob([`\uFEFF${csv}`], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `sellerpulse-financial-report-${new Date().toISOString().slice(0, 10)}.csv`;
    link.click();
    URL.revokeObjectURL(url);
  }

  const statuses = useMemo(() => {
    return [...new Set((reportData.items || []).map((item) => item.status).filter(Boolean))];
  }, [reportData.items]);

  const filteredItems = useMemo(() => {
    const search = query.trim().toLowerCase();
    return [...(reportData.items || [])]
      .filter((item) => statusFilter === "all" || item.status === statusFilter)
      .filter((item) => {
        if (!search) return true;
        return [item.nm_id, item.vendor_code, item.product_name].some((value) => String(value || "").toLowerCase().includes(search));
      })
      .sort((a, b) => {
        const left = a.profit ?? (sortProfit === "asc" ? Number.NEGATIVE_INFINITY : Number.POSITIVE_INFINITY);
        const right = b.profit ?? (sortProfit === "asc" ? Number.NEGATIVE_INFINITY : Number.POSITIVE_INFINITY);
        return sortProfit === "asc" ? left - right : right - left;
      });
  }, [reportData.items, query, statusFilter, sortProfit]);

  const filteredRawRows = useMemo(() => {
    const search = rawQuery.trim().toLowerCase();
    const rows = reportData.raw_rows || [];
    if (!search) return rows;
    return rows.filter((row) =>
      [row.nm_id, row.vendor_code, row.product_name].some((v) => String(v || "").toLowerCase().includes(search))
    );
  }, [reportData.raw_rows, rawQuery]);

  const rawTotalPages = Math.max(1, Math.ceil(filteredRawRows.length / RAW_PAGE_SIZE));
  const rawPageRows = filteredRawRows.slice((rawPage - 1) * RAW_PAGE_SIZE, rawPage * RAW_PAGE_SIZE);

  const shop = { name: "Финансовый отчёт WB", token_status: "active" };
  const report = reportData.report;

  return (
    <AppShell
      user={user}
      active="financial-report"
      onLogout={onLogout}
      onNavigate={onNavigate}
      shop={shop}
      searchValue={activeTab === "raw" ? rawQuery : query}
      onSearchChange={activeTab === "raw" ? (v) => { setRawQuery(v); setRawPage(1); } : setQuery}
      searchPlaceholder="Поиск по nmID, артикулу или названию..."
    >
      <section className="page-title report-title">
        <div>
          <p className="eyebrow">Файл WB</p>
          <h1>Финансовый отчёт WB</h1>
          <p>Загрузите Excel, CSV или ZIP-файл финансового отчёта Wildberries. SellerPulse покажет исходный отчёт и рассчитает РНП — Руку на пульсе.</p>
        </div>
      </section>

      {(message || reportData.message) && <div className="notice info">{message || reportData.message}</div>}
      {error && <div className="notice warning"><AlertTriangle size={18} /> {error}</div>}

      {/* Блок 1: Загрузка файла */}
      <section className="report-upload-grid">
        <div className="panel upload-panel">
          <div
            className={`dropzone ${dragging ? "dragging" : ""}`}
            onDragOver={(event) => { event.preventDefault(); setDragging(true); }}
            onDragLeave={() => setDragging(false)}
            onDrop={(event) => {
              event.preventDefault();
              setDragging(false);
              selectFile(event.dataTransfer.files?.[0]);
            }}
          >
            <UploadCloud size={34} />
            <h2>Финансовый отчёт WB</h2>
            <p>Загрузите Excel, CSV или ZIP-файл финансового отчёта Wildberries. SellerPulse покажет исходный отчёт и рассчитает РНП — Руку на пульсе.</p>
            <label className="file-picker">
              <input type="file" accept=".xlsx,.xls,.csv,.zip" onChange={(event) => selectFile(event.target.files?.[0])} />
              Выбрать файл
            </label>
            {file && <strong className="selected-file">{file.name}</strong>}
          </div>
          <div className="actions">
            <button type="button" className="primary" onClick={upload} disabled={uploading || !file}><FileSpreadsheet size={17} /> {uploading ? "Загружаем..." : "Загрузить и рассчитать"}</button>
            <button type="button" onClick={validateFile} disabled={validating || !file}><ShieldCheck size={17} /> {validating ? "Проверяем..." : "Проверить точность"}</button>
            <button type="button" onClick={load} disabled={loading}><RefreshCcw size={17} /> Обновить</button>
          </div>
        </div>

        <aside className="panel report-summary">
          <div className="panel-head">
            <div>
              <h2>Статус обработки</h2>
              <p>{report ? "Последний отчёт загружен и рассчитан." : "Финансовый отчёт ещё не загружен."}</p>
            </div>
            <FileSpreadsheet size={22} />
          </div>
          <InfoLine label="Файл" value={report?.file_name} />
          <InfoLine label="Период" value={reportPeriod(report)} />
          <InfoLine label="Строк" value={report?.rows_count} />
          <InfoLine label="Товаров" value={report?.products_count} />
          <InfoLine label="Статус" value={report?.status === "processed" ? "Обработан" : null} />
          <InfoLine label="Без себестоимости" value={report?.missing_costs} />
          <div className="report-tax-box">
            <InfoLine label="Налог" value={report?.tax_percent != null ? `${report.tax_percent}%` : "Не указан"} />
            <InfoLine label="Реклама" value={report?.manual_advertising != null ? money(report.manual_advertising) : null} />
            <InfoLine label="Прочие расходы" value={report?.manual_other_expenses != null ? money(report.manual_other_expenses) : null} />
          </div>
        </aside>
      </section>

      {/* Вкладки */}
      <div className="report-tabs">
        <button className={`report-tab${activeTab === "raw" ? " active" : ""}`} onClick={() => setActiveTab("raw")}>
          <Database size={16} /> Исходный отчёт
          {report && <span className="tab-badge">{report.rows_count}</span>}
        </button>
        <button className={`report-tab${activeTab === "rnp" ? " active" : ""}`} onClick={() => setActiveTab("rnp")}>
          <Activity size={16} /> Рука на пульсе
          {report && <span className="tab-badge">{report.products_count}</span>}
        </button>
        <button className={`report-tab${activeTab === "accuracy" ? " active" : ""}`} onClick={() => setActiveTab("accuracy")}>
          <ShieldCheck size={16} /> Проверка точности
        </button>
      </div>

      {/* Вкладка 1: Исходный отчёт */}
      {activeTab === "raw" && (
        <section className="panel report-table-panel">
          <div className="panel-head report-panel-head">
            <div>
              <h2>Исходный финансовый отчёт</h2>
              <p>
                {report
                  ? `Файл: ${report.file_name} · Период: ${reportPeriod(report) || "—"} · Строк: ${report.rows_count} · Товаров: ${report.products_count}`
                  : "Загрузите финансовый отчёт WB, чтобы увидеть исходные строки."}
              </p>
            </div>
          </div>
          <div className="report-toolbar">
            <label className="search-box report-search">
              <Search size={18} />
              <input
                placeholder="Поиск по nmID, артикулу или названию..."
                value={rawQuery}
                onChange={(event) => { setRawQuery(event.target.value); setRawPage(1); }}
              />
            </label>
            <span>{filteredRawRows.length} строк</span>
            {rawTotalPages > 1 && (
              <div className="pagination">
                <button disabled={rawPage <= 1} onClick={() => setRawPage(rawPage - 1)}>‹</button>
                <span>{rawPage} / {rawTotalPages}</span>
                <button disabled={rawPage >= rawTotalPages} onClick={() => setRawPage(rawPage + 1)}>›</button>
              </div>
            )}
          </div>
          {loading ? (
            <div className="loader inline">Загружаем отчёт</div>
          ) : (
            <div className="table-wrap report-table-wrap">
              <table className="pulse-table report-table raw-report-table">
                <thead>
                  <tr>
                    <th>Дата</th>
                    <th>nmID</th>
                    <th>Артикул</th>
                    <th>Название товара</th>
                    <th>Обоснование для оплаты</th>
                    <th>Кол-во</th>
                    <th>Цена розн.</th>
                    <th>Сумма продажи</th>
                    <th>К перечислению</th>
                    <th>Комиссия WB</th>
                    <th>Логистика</th>
                    <th>Хранение</th>
                    <th>Возвраты</th>
                    <th>Эквайринг</th>
                    <th>СПА</th>
                    <th>Штрафы</th>
                    <th>Удержания</th>
                    <th>Прочие</th>
                  </tr>
                </thead>
                <tbody>
                  {rawPageRows.map((row, index) => (
                    <tr key={index} className={row.is_sale ? "row-sale" : ""}>
                      <td>{row.date || "—"}</td>
                      <td>{row.nm_id || "—"}</td>
                      <td>{row.vendor_code || "—"}</td>
                      <td className="product-cell"><strong>{row.product_name || "—"}</strong></td>
                      <td><span className={`op-tag ${row.is_sale ? "op-sale" : "op-other"}`}>{row.operation || "—"}</span></td>
                      <td>{row.quantity ?? "—"}</td>
                      <td>{money(row.retail_price)}</td>
                      <td>{money(row.sales_amount)}</td>
                      <td>{money(row.to_pay)}</td>
                      <td>{money(row.commission)}</td>
                      <td>{money(row.logistics)}</td>
                      <td>{money(row.storage)}</td>
                      <td>{row.returns ?? "—"}</td>
                      <td>{money(row.acquiring)}</td>
                      <td>{money(row.spa)}</td>
                      <td>{money(row.penalties)}</td>
                      <td>{money(row.deductions)}</td>
                      <td>{money(row.other_expenses)}</td>
                    </tr>
                  ))}
                  {!rawPageRows.length && (
                    <tr>
                      <td colSpan="18" className="empty">
                        {(reportData.raw_rows || []).length
                          ? "По запросу ничего не найдено."
                          : "Загрузите финансовый отчёт WB, чтобы увидеть исходные строки."}
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
          {rawTotalPages > 1 && (
            <div className="pagination pagination-bottom">
              <button disabled={rawPage <= 1} onClick={() => setRawPage(rawPage - 1)}>‹ Назад</button>
              <span>Страница {rawPage} из {rawTotalPages} · {filteredRawRows.length} строк</span>
              <button disabled={rawPage >= rawTotalPages} onClick={() => setRawPage(rawPage + 1)}>Вперёд ›</button>
            </div>
          )}
        </section>
      )}

      {/* Вкладка 2: Рука на пульсе */}
      {activeTab === "rnp" && (
        <section className="panel report-table-panel">
          <div className="panel-head report-panel-head">
            <div>
              <h2>Рука на пульсе по финансовому отчёту</h2>
              <p>Сгруппированная таблица по товарам на основе финансового отчёта WB.</p>
            </div>
            <div className="report-actions">
              <button type="button" onClick={exportCsv} disabled={!filteredItems.length}><Download size={17} /> Экспорт в Excel</button>
              <button type="button" className="danger" onClick={clearReport} disabled={!report}><Trash2 size={17} /> Очистить отчёт</button>
            </div>
          </div>

          <div className="report-settings-block">
            <div className="report-settings-title">
              <strong>Настройки расчёта РНП</strong>
              <p>Задайте параметры один раз и нажмите «Применить». SellerPulse пересчитает всю таблицу.</p>
            </div>
            <div className="report-settings-inputs">
              <label className="settings-field">
                <span>Себестоимость для всех, ₽</span>
                <input type="number" min="0" step="0.01" placeholder="Напр. 200" value={settingsCost}
                  onChange={(e) => setSettingsCost(e.target.value)} disabled={!report} />
              </label>
              <label className="settings-field">
                <span>Налог, %</span>
                <input type="number" min="0" max="100" step="0.1" placeholder="Напр. 22" value={reportTax}
                  onChange={(e) => setReportTax(e.target.value)} disabled={!report} />
              </label>
              <label className="settings-field">
                <span>Реклама, ₽ (итого)</span>
                <input type="number" min="0" step="0.01" placeholder="Не обязательно" value={settingsAdv}
                  onChange={(e) => setSettingsAdv(e.target.value)} disabled={!report} />
              </label>
              <label className="settings-field">
                <span>Прочие расходы, ₽</span>
                <input type="number" min="0" step="0.01" placeholder="Не обязательно" value={settingsOther}
                  onChange={(e) => setSettingsOther(e.target.value)} disabled={!report} />
              </label>
            </div>
            <button type="button" className="primary" onClick={applySettings} disabled={!report || applyingSettings}>
              <RefreshCcw size={16} /> {applyingSettings ? "Пересчитываем..." : "Применить и пересчитать"}
            </button>
          </div>

          <div className="report-toolbar">
            <label className="search-box report-search">
              <Search size={18} />
              <input placeholder="Поиск по nmID, артикулу или названию..." value={query} onChange={(event) => setQuery(event.target.value)} />
            </label>
            <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
              <option value="all">Все статусы</option>
              {statuses.map((status) => <option key={status} value={status}>{status}</option>)}
            </select>
            <select value={sortProfit} onChange={(event) => setSortProfit(event.target.value)}>
              <option value="asc">Прибыль: сначала минус</option>
              <option value="desc">Прибыль: сначала плюс</option>
            </select>
            <span>{filteredItems.length} товаров</span>
          </div>

          {loading ? <div className="loader inline">Загружаем отчёт</div> : (
            <div className="table-wrap report-table-wrap">
              <table className="pulse-table report-table">
                <thead>
                  <tr>
                    {["Артикул", "Товар / nmID", "Продано", "Возвраты", "Заказано", "% выкупа", "До СПП", "СПП", "После СПП", "К перечислению", "Себестоимость", "Комиссия WB", "Логистика", "Хранение", "Эквайринг", "СПА", "Реклама", "Налог", "Штрафы", "Удержания", "Прочие расходы", "Прибыль", "Прибыль за штуку", "Маржа", "ДРР", "Статус", "Действие"].map((head) => <th key={head}>{head}</th>)}
                  </tr>
                </thead>
                <tbody>
                  {filteredItems.map((item) => (
                    <tr key={item.id}>
                      <td>{item.vendor_code || "?"}</td>
                      <td className="product-cell"><strong>{item.product_name}</strong><small>nmID {item.nm_id || "?"}</small></td>
                      <td>{number(item.sold_qty)}</td>
                      <td>{item.returns > 0 ? number(item.returns) : "—"}</td>
                      <td>{item.orders_qty > 0 ? number(item.orders_qty) : "—"}</td>
                      <td>{item.buyout_rate != null ? `${item.buyout_rate}%` : "—"}</td>
                      <td>{money(item.before_spp)}</td>
                      <td>{money(item.spp_amount)}</td>
                      <td>{money(item.after_spp)}</td>
                      <td>{money(item.to_pay)}</td>
                      <td>
                        <RnpCostCell item={item} costs={costs} setCosts={setCosts} saving={saving} onSave={saveCost} />
                      </td>
                      <td>{money(item.commission)}</td>
                      <td>{money(item.logistics)}</td>
                      <td>{money(item.storage)}</td>
                      <td>{money(item.acquiring)}</td>
                      <td>{money(item.spa)}</td>
                      <td>{money(item.advertising)}</td>
                      <td>{item.tax != null ? money(item.tax) : <span className="no-data-label">Не указан</span>}</td>
                      <td>{money(item.penalties)}</td>
                      <td>{money(item.deductions)}</td>
                      <td>{money(item.other_expenses)}</td>
                      <td className={item.profit != null ? (item.profit < 0 ? "cell-negative" : "cell-positive") : ""}>{item.profit != null ? money(item.profit) : <span className="no-data-label">Нет данных</span>}</td>
                      <td className={item.profit_per_unit != null ? (item.profit_per_unit < 0 ? "cell-negative" : "cell-positive") : ""}>{item.profit_per_unit != null ? money(item.profit_per_unit) : <span className="no-data-label">Нет данных</span>}</td>
                      <td className={item.margin != null ? (item.margin < 0 ? "cell-negative" : item.margin < 15 ? "cell-warn" : "cell-positive") : ""}>{percent(item.margin)}</td>
                      <td>{percent(item.drr)}</td>
                      <td><span className={`status ${statusClass(item.status)}`}>{item.status}</span></td>
                      <td>{item.action}</td>
                    </tr>
                  ))}
                  {!filteredItems.length && <tr><td colSpan="27" className="empty">Загрузите финансовый отчёт WB, чтобы построить таблицу.</td></tr>}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      {/* Вкладка 3: Проверка точности */}
      {activeTab === "accuracy" && (
        <AccuracyCheck validation={accuracy || reportData.validation} />
      )}
    </AppShell>
  );
}

const ACCURACY_ROWS = [
  ["total_sales", "Сумма продаж"],
  ["total_for_pay", "К перечислению"],
  ["total_commission", "Комиссия WB"],
  ["total_logistics", "Логистика"],
  ["total_storage", "Хранение"],
  ["total_acquiring", "Эквайринг"],
  ["total_spa", "СПА"],
  ["total_penalties", "Штрафы"],
  ["total_deductions", "Удержания"],
];

function AccuracyCheck({ validation }) {
  const hasValidation = Boolean(validation);
  const diff = validation?.diff || {};
  const ok = hasValidation && Object.values(diff).every((value) => Math.abs(Number(value || 0)) < 0.01);
  const foundCount = validation?.columns_found
    ? Object.values(validation.columns_found).filter(Boolean).length
    : 0;

  return (
    <section className="panel accuracy-panel">
      <div className="panel-head report-panel-head">
        <div>
          <h2>Проверка точности</h2>
          <p>Сверяем суммы из Excel WB с суммами после группировки по товарам.</p>
        </div>
        <span className={`status ${!hasValidation ? "warn" : ok ? "ok" : "bad"}`}>
          {!hasValidation ? "Загрузите финансовый отчёт WB" : ok ? "Расчёты совпадают с финансовым отчётом WB" : "Есть расхождение с отчётом WB"}
        </span>
      </div>

      {hasValidation ? (
        <>
          <div className="accuracy-meta">
            <InfoLine label="Строк в отчёте" value={validation.rows_count} />
            <InfoLine label="Товаров найдено" value={validation.products_count} />
            <InfoLine label="Колонок найдено" value={foundCount} />
          </div>
          <div className="table-wrap accuracy-table-wrap">
            <table className="accuracy-table">
              <thead>
                <tr>
                  <th>Показатель</th>
                  <th>Excel WB</th>
                  <th>После группировки</th>
                  <th>Разница</th>
                </tr>
              </thead>
              <tbody>
                {ACCURACY_ROWS.map(([key, label]) => (
                  <tr key={key}>
                    <td>{label}</td>
                    <td>{moneyPrecise(validation.totals_from_excel?.[key])}</td>
                    <td>{moneyPrecise(validation.totals_after_grouping?.[key])}</td>
                    <td className={Math.abs(Number(diff[key] || 0)) < 0.01 ? "diff-ok" : "diff-bad"}>{moneyPrecise(diff[key])}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      ) : (
        <div className="empty accuracy-empty">Загрузите или проверьте Excel-файл WB, чтобы увидеть сверку точности.</div>
      )}
    </section>
  );
}

function reportPeriod(report) {
  if (!report?.period_start && !report?.period_end) return null;
  if (report.period_start === report.period_end) return report.period_start;
  return `${report.period_start || "?"} — ${report.period_end || "?"}`;
}

function moneyPrecise(value) {
  if (value === null || value === undefined) return "Нет данных WB";
  return new Intl.NumberFormat("ru-RU", {
    style: "currency",
    currency: "RUB",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(Number(value));
}

function parseCost(value) {
  const normalized = normalizeCostInput(value);
  if (normalized === "") return { ok: true, value: null };
  const nextCost = Number(normalized);
  if (!Number.isFinite(nextCost) || nextCost < 0) {
    return { ok: false, message: "Введите корректную себестоимость: число больше или равно 0." };
  }
  return { ok: true, value: nextCost };
}

function normalizeCostInput(value) {
  if (value === null || value === undefined) return "";
  const raw = String(value).replace(",", ".").trim();
  if (raw === "") return "";
  const numeric = Number(raw);
  return Number.isFinite(numeric) ? String(numeric) : raw;
}

function normalizeCostValue(value) {
  if (value === null || value === undefined) return "";
  const numeric = Number(value);
  return Number.isFinite(numeric) ? String(numeric) : String(value);
}

function AppShell({
  children,
  user,
  active,
  onLogout,
  onNavigate,
  shop,
  onSync,
  syncing,
  searchValue = "",
  onSearchChange,
  searchPlaceholder = "Поиск товаров, артикулов, nmID...",
}) {
  const [notificationsOpen, setNotificationsOpen] = useState(false);
  const [accountOpen, setAccountOpen] = useState(false);
  const canSearch = typeof onSearchChange === "function";

  function goTo(screen) {
    setAccountOpen(false);
    onNavigate(screen);
  }

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <BrandMark />
        <nav className="side-nav">
          <NavItem icon={Gauge} label="Главная" active={active === "dashboard"} onClick={() => onNavigate("dashboard")} />
          <NavItem icon={KeyRound} label="Токен WB" active={active === "token"} onClick={() => onNavigate("token")} />
          <NavItem icon={WalletCards} label="Себестоимость" active={active === "costs"} onClick={() => onNavigate("costs")} />
          <NavItem icon={FileSpreadsheet} label="Финансовый отчёт" active={active === "financial-report"} onClick={() => onNavigate("financial-report")} />
          {user?.is_admin && <NavItem icon={BriefcaseBusiness} label="Администратор" active={active === "admin"} onClick={() => onNavigate("admin")} />}
        </nav>
        <div className="sidebar-footer">
          {user?.subscription && (
            <div className={`sidebar-sub-widget ${user.subscription.active ? "" : "sidebar-sub-widget--expired"}`}>
              <div className="sidebar-sub-icon">
                {user.subscription.active ? <CheckCircle2 size={15} /> : <AlertTriangle size={15} />}
              </div>
              <div>
                <strong>{user.subscription.status === "trial" ? "Пробный период" : user.subscription.active ? "Подписка активна" : "Подписка истекла"}</strong>
                <small>{user.subscription.days_left != null ? `Осталось ${user.subscription.days_left} дн.` : "Нет данных"}</small>
              </div>
            </div>
          )}
          {shop?.name && (
            <div className="sidebar-shop-widget">
              <span className={`status-dot status-dot--${shop?.token_status === "active" ? "ok" : "warn"}`} />
              <div>
                <strong>{shop.name}</strong>
                <small>{STATUS_META[shop?.token_status]?.text || "WB кабинет"}</small>
              </div>
            </div>
          )}
          <button className="side-logout" onClick={onLogout}><LogOut size={17} /> Выйти</button>
        </div>
      </aside>

      <section className="workspace">
        <header className="topbar">
          <label className={`search-box ${canSearch ? "" : "disabled"}`}>
            <Search size={18} />
            <input
              placeholder={canSearch ? searchPlaceholder : "Поиск доступен на страницах с таблицами"}
              value={searchValue}
              onChange={(event) => onSearchChange?.(event.target.value)}
              disabled={!canSearch}
            />
          </label>
          <div className="top-actions">
            {onSync && <button className="top-sync" onClick={onSync} disabled={syncing}><RefreshCcw size={17} /> {syncing ? "Синхронизация" : "Синхронизировать"}</button>}
            <div className="top-action-menu">
              <button
                type="button"
                className={`icon-btn ${notificationsOpen ? "active" : ""}`}
                aria-label="Уведомления"
                onClick={() => {
                  setNotificationsOpen((current) => !current);
                  setAccountOpen(false);
                }}
              >
                <Bell size={18} />
              </button>
              {notificationsOpen && (
                <div className="top-popover notification-popover">
                  <strong>Состояние Dashboard</strong>
                  <span>{syncing ? "Синхронизация выполняется." : "Синхронизация не запущена."}</span>
                  <span>Токен: {STATUS_META[shop?.token_status]?.text || shop?.token_status || "не подключён"}</span>
                  <span>Кабинет: {shop?.name || user?.name || "SellerPulse"}</span>
                </div>
              )}
            </div>
            <div className="top-action-menu">
              <button
                type="button"
                className={`account-chip ${accountOpen ? "active" : ""}`}
                onClick={() => {
                  setAccountOpen((current) => !current);
                  setNotificationsOpen(false);
                }}
              >
                <span className={`status-dot status-dot--${shop?.token_status === "active" ? "ok" : "warn"}`} />
                <span>
                  <strong>{shop?.name || user?.name || "SellerPulse"}</strong>
                  <small>{STATUS_META[shop?.token_status]?.text || shop?.token_status || "WB кабинет"}</small>
                </span>
                <ChevronDown size={16} />
              </button>
              {accountOpen && (
                <div className="top-popover account-popover">
                  <button type="button" onClick={() => goTo("dashboard")}><Gauge size={16} /> Главная</button>
                  <button type="button" onClick={() => goTo("token")}><KeyRound size={16} /> Токен WB</button>
                  <button type="button" onClick={() => goTo("costs")}><WalletCards size={16} /> Себестоимость</button>
                  <button type="button" onClick={() => goTo("financial-report")}><FileSpreadsheet size={16} /> Финансовый отчёт</button>
                  <button type="button" className="danger-menu" onClick={onLogout}><LogOut size={16} /> Выйти</button>
                </div>
              )}
            </div>
          </div>
        </header>
        <div className="mobile-nav">
          <NavItem icon={Gauge} label="Главная" active={active === "dashboard"} onClick={() => onNavigate("dashboard")} />
          <NavItem icon={KeyRound} label="Токен WB" active={active === "token"} onClick={() => onNavigate("token")} />
          <NavItem icon={WalletCards} label="Себестоимость" active={active === "costs"} onClick={() => onNavigate("costs")} />
          <NavItem icon={FileSpreadsheet} label="Финансовый отчёт" active={active === "financial-report"} onClick={() => onNavigate("financial-report")} />
          <button onClick={onLogout}><LogOut size={17} /> Выйти</button>
        </div>
        <div className="content">{children}</div>
      </section>
    </main>
  );
}

function BrandMark() {
  return (
    <div className="brand">
      <span className="brand-icon"><Activity size={22} /></span>
      <span><strong>SellerPulse</strong><small>Аналитика WB</small></span>
    </div>
  );
}

function Benefit({ icon: Icon, title, text }) {
  return (
    <div className="benefit">
      <Icon size={18} />
      <span><strong>{title}</strong><small>{text}</small></span>
    </div>
  );
}

function NavItem({ icon: Icon, label, active, onClick }) {
  return (
    <button className={`nav-item ${active ? "active" : ""}`} onClick={onClick} type="button">
      <span className="nav-icon"><Icon size={17} /></span>
      {label}
    </button>
  );
}

function Field({ icon: Icon, label, type = "text", value, onChange, required }) {
  return (
    <label className="field">
      <span>{label}</span>
      <div className="input-shell">
        {Icon && <Icon size={17} />}
        <input type={type} required={required} value={value} onChange={(event) => onChange(event.target.value)} />
      </div>
    </label>
  );
}

function InfoLine({ label, value }) {
  return (
    <div className="info-line">
      <span>{label}</span>
      <strong>{value ?? "Нет данных WB"}</strong>
    </div>
  );
}

function RightsList({ permissions }) {
  const items = permissions?.items;
  if (items && typeof items === "object") {
    return (
      <div className="rights-list">
        {Object.values(items).map((item) => (
          <span key={item.title} className={item.has_access ? "ok" : "missing"}>
            <CheckCircle2 size={15} /> {item.title}
          </span>
        ))}
      </div>
    );
  }
  return (
    <div className="rights-list">
      {REQUIRED_RIGHTS.map((item) => <span key={item}><CheckCircle2 size={15} /> {item}</span>)}
    </div>
  );
}

function StatusPill({ status }) {
  const meta = STATUS_META[status] || STATUS_META.invalid;
  return <span className={`pill ${meta.className}`}>{meta.label}</span>;
}

function statusText(status) {
  return STATUS_META[status]?.text || "Токен не подключён";
}

function Metric({ icon: Icon, label, value, trend }) {
  return (
    <article className="metric">
      <div className="metric-icon"><Icon size={20} /></div>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{trend}</small>
    </article>
  );
}

const SYNC_STEP_LABELS = {
  token_check: "Проверка токена",
  products: "Товары",
  stocks: "Остатки",
  sales: "Продажи",
  orders: "Заказы",
  finance_reports: "Финансовые отчёты WB",
  advertising: "Реклама",
  dashboard_calc: "Расчёт аналитики",
};

const SYNC_TYPE_LABELS = {
  initial_full_sync: "Первичная синхронизация WB",
  manual_sync: "Синхронизация WB API",
  daily_sync: "Ежедневная синхронизация",
  auto_sync: "Автоматическая синхронизация",
};

function SyncProgressBanner({ syncStatus, onRestart }) {
  if (!syncStatus || syncStatus.status === "not_started") return null;
  const isActive = ["queued", "running", "partial"].includes(syncStatus.status);
  const isCompleted = syncStatus.status === "completed";
  const isFailed = syncStatus.status === "failed";

  const stepIcon = (s) => {
    if (s === "completed") return <CheckCircle2 size={14} className="step-icon ok" />;
    if (s === "running") return <RefreshCcw size={14} className="step-icon spin" />;
    if (s === "failed") return <AlertTriangle size={14} className="step-icon bad" />;
    if (s === "skipped") return <AlertTriangle size={14} className="step-icon warn" />;
    return <span className="step-icon dot">○</span>;
  };

  if (isCompleted) return null;

  return (
    <section className={`sync-progress-banner ${isFailed ? "sync-banner--failed" : ""}`}>
      <div className="sync-banner-header">
        <div className="sync-banner-title">
          {isActive && <RefreshCcw size={16} className="spin" />}
          {isFailed && <AlertTriangle size={16} />}
          <strong>{SYNC_TYPE_LABELS[syncStatus.sync_type] || "Синхронизация WB"}</strong>
          <span className="sync-banner-status">{syncStatus.progress_percent}%</span>
        </div>
        <div className="sync-progress-bar">
          <div className="sync-progress-fill" style={{ width: `${syncStatus.progress_percent}%` }} />
        </div>
      </div>

      {syncStatus.steps && syncStatus.steps.length > 0 && (
        <div className="sync-steps-grid">
          {syncStatus.steps.map((step) => (
            <div key={step.step_name} className={`sync-step sync-step--${step.status}`}>
              {stepIcon(step.status)}
              <span className="sync-step-name">{SYNC_STEP_LABELS[step.step_name] || step.step_name}</span>
              {step.records_saved != null && (
                <span className="sync-step-count">{step.records_saved}</span>
              )}
            </div>
          ))}
        </div>
      )}

      {isActive && (
        <p className="sync-banner-hint">
          Синхронизация обычно занимает 2–5 минут. Можно закрыть страницу — синхронизация продолжится автоматически.
        </p>
      )}
      {isFailed && (
        <div className="sync-banner-failed-row">
          {syncStatus.last_error && (
            <p className="sync-banner-hint sync-banner-hint--error">{syncStatus.last_error}</p>
          )}
          {onRestart && (
            <button className="btn-secondary btn-sm" onClick={onRestart}>
              <RefreshCcw size={13} /> Перезапустить
            </button>
          )}
        </div>
      )}
    </section>
  );
}

function SyncSources({ sources }) {
  const entries = Object.entries(sources);
  if (!entries.length) return null;

  return (
    <section className="sync-sources">
      {entries.map(([key, source]) => {
        const status = source.status || "api_error";
        const rows = source.rows ?? 0;
        const text = status === "ok" ? `${SOURCE_STATUS_TEXT.ok}: ${rows}` : SOURCE_STATUS_TEXT[status] || "не получено";
        return (
          <span key={key} className={`source-chip ${status}`}>
            <strong>{SOURCE_LABELS[key] || key}</strong>
            {text}
          </span>
        );
      })}
    </section>
  );
}

function Expenses({ expenses, revenue }) {
  return (
    <section className="panel expenses-panel">
      <div className="panel-head">
        <div>
          <h2>Где магазин тратит деньги</h2>
          <p>Расходы за выбранный период. Если WB не вернул данные, показываем "Нет данных WB".</p>
        </div>
        <Boxes size={22} />
      </div>
      <div className="expenses-grid">
        {Object.entries(EXPENSE_LABELS).map(([key, label]) => (
          <div key={key} className="expense-card">
            <span>{label}</span>
            <strong>{money(expenses?.[key])}</strong>
            <small>{expenseShare(expenses?.[key], revenue)}</small>
          </div>
        ))}
      </div>
    </section>
  );
}

function expenseShare(value, revenue) {
  if (value === null || value === undefined || !revenue) return "Нет данных WB";
  return `${percent((value / revenue) * 100)} от выручки`;
}

function FinancialPulseTable({ rows, allRows, totalRows, query, onQueryChange, reload }) {
  const soldRows = allRows.filter((r) => (r.sold_qty || 0) > 0);
  const totalSoldQty = soldRows.reduce((s, r) => s + (r.sold_qty || 0), 0);
  const totalSalesSum = allRows.reduce((s, r) => r.sales_sum != null ? s + r.sales_sum : s, 0);
  const totalToPay = allRows.reduce((s, r) => r.to_pay != null ? s + r.to_pay : s, 0);
  const totalProfit = soldRows.filter((r) => r.profit != null).reduce((s, r) => s + r.profit, 0);
  const avgProfitPerUnit = totalSoldQty > 0 && soldRows.some((r) => r.profit != null) ? totalProfit / totalSoldQty : null;
  const inPlusCount = soldRows.filter((r) => r.profit != null && r.profit >= 0).length;
  const inMinusCount = soldRows.filter((r) => r.profit != null && r.profit < 0).length;
  const expensesNoSalesCount = allRows.filter((r) => r.status === "Расходы без продаж").length;

  return (
    <section className="panel pulse-panel">
      <div className="panel-head">
        <div>
          <h2>Рука на пульсе</h2>
          <p>Товары из финансового отчёта WB — точные данные по прибыли с каждой продажи.</p>
        </div>
        <span className="source-badge source-badge--exact"><CheckCircle2 size={14} /> Финансовый отчёт WB</span>
      </div>

      <div className="fp-summary-cards">
        <div className="fp-summary-card">
          <span className="fp-summary-label">Продано, шт.</span>
          <span className="fp-summary-value">{number(totalSoldQty)}</span>
        </div>
        <div className="fp-summary-card">
          <span className="fp-summary-label">Сумма продаж</span>
          <span className="fp-summary-value">{money(totalSalesSum)}</span>
        </div>
        <div className="fp-summary-card">
          <span className="fp-summary-label">К перечислению</span>
          <span className="fp-summary-value">{money(totalToPay)}</span>
        </div>
        <div className="fp-summary-card">
          <span className="fp-summary-label">Чистая прибыль</span>
          <span className="fp-summary-value">{money(totalProfit)}</span>
        </div>
        <div className="fp-summary-card fp-summary-card--accent">
          <span className="fp-summary-label">Прибыль с 1 шт.</span>
          <span className="fp-summary-value">{money(avgProfitPerUnit)}</span>
        </div>
        <div className="fp-summary-card fp-summary-card--plus">
          <span className="fp-summary-label">В плюсе</span>
          <span className="fp-summary-value">{inPlusCount}</span>
        </div>
        <div className="fp-summary-card fp-summary-card--minus">
          <span className="fp-summary-label">В минусе</span>
          <span className="fp-summary-value">{inMinusCount}</span>
        </div>
        {expensesNoSalesCount > 0 && (
          <div className="fp-summary-card fp-summary-card--warning">
            <span className="fp-summary-label">Расходы без продаж</span>
            <span className="fp-summary-value">{expensesNoSalesCount}</span>
          </div>
        )}
      </div>

      <div className="pulse-toolbar">
        <label className="search-box pulse-search">
          <Search size={18} />
          <input
            placeholder="Быстрый поиск..."
            value={query}
            onChange={(event) => onQueryChange(event.target.value)}
          />
        </label>
        <span>{rows.length} из {totalRows} товаров</span>
      </div>
      <div className="table-wrap">
        <table className="pulse-table fp-table">
          <thead>
            <tr>
              <th>Артикул</th>
              <th>Товар / nmID</th>
              <th>Продано</th>
              <th>Сумма продаж</th>
              <th>Ср. цена</th>
              <th>До СПП</th>
              <th>СПП</th>
              <th>После СПП</th>
              <th>К перечислению</th>
              <th>Себест. за 1 шт.</th>
              <th>Себест. итого</th>
              <th title="Информационно — уже учтена в «К перечислению»">Комиссия WB ℹ</th>
              <th>Логистика</th>
              <th>Хранение</th>
              <th>Эквайринг</th>
              <th>СПА</th>
              <th>Реклама</th>
              <th>Налог</th>
              <th>Штрафы</th>
              <th>Удержания</th>
              <th>Прочие расходы</th>
              <th>Прибыль всего</th>
              <th className="col-profit-unit-head">Прибыль с 1 шт.</th>
              <th>Маржа</th>
              <th>Статус</th>
              <th>Действие</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => <FinancialPulseRow key={row.nm_id} row={row} reload={reload} />)}
            {!rows.length && (
              <tr><td colSpan="26" className="empty">
                {totalRows ? "По выбранным фильтрам товары не найдены." : "Загрузите финансовый отчёт WB, чтобы увидеть данные."}
              </td></tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function FinancialPulseRow({ row }) {
  const [detailsOpen, setDetailsOpen] = useState(false);
  const noCost = row.unit_cost_price == null;
  const avgPrice = row.sold_qty && row.after_spp != null ? row.after_spp / row.sold_qty : null;
  const profitCls = row.profit != null && row.profit < 0 ? "cell-negative" : row.profit != null && row.profit > 0 ? "cell-positive" : "";
  const ppuCls = `col-profit-unit${row.profit_per_unit != null && row.profit_per_unit < 0 ? " cell-negative" : row.profit_per_unit != null && row.profit_per_unit > 0 ? " cell-positive" : ""}`;

  return (
    <>
      <tr>
        <td>{row.vendor_code || "—"}</td>
        <td className="product-cell"><strong>{row.name}</strong><small>nmID {row.nm_id}</small></td>
        <td>{number(row.sold_qty)}</td>
        <td>{money(row.sales_sum)}</td>
        <td>{money(avgPrice)}</td>
        <td>{money(row.before_spp)}</td>
        <td>{money(row.spp)}</td>
        <td>{money(row.after_spp)}</td>
        <td>{money(row.to_pay)}</td>
        <td>{noCost ? <span className="no-cost-hint">Введите себестоимость</span> : money(row.unit_cost_price)}</td>
        <td>{noCost ? "—" : money(row.cost_price)}</td>
        <td className="cell-info">{money(row.commission)}</td>
        <td>{money(row.logistics)}</td>
        <td>{money(row.storage)}</td>
        <td>{money(row.acquiring)}</td>
        <td>{money(row.spa)}</td>
        <td>{money(row.advertising)}</td>
        <td>{row.tax != null ? money(row.tax) : <span className="no-data-label">Не указан</span>}</td>
        <td>{money(row.penalties)}</td>
        <td>{money(row.deductions)}</td>
        <td>{money(row.other_expenses)}</td>
        <td className={profitCls}>{noCost ? "—" : money(row.profit)}</td>
        <td className={ppuCls}>{noCost ? <span className="no-cost-hint">Нет себестоимости</span> : money(row.profit_per_unit)}</td>
        <td>{noCost ? "—" : percent(row.margin)}</td>
        <td><span className={`status ${statusClass(row.status)}`}>{row.status}</span></td>
        <td><button type="button" className="small" onClick={() => setDetailsOpen((v) => !v)}>{detailsOpen ? "Скрыть" : row.action}</button></td>
      </tr>
      {detailsOpen && (
        <tr className="detail-row">
          <td colSpan="26">
            <div className="row-detail">
              <strong>{row.action}</strong>
              <span>{financialActionHint(row)}</span>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

function financialActionHint(row) {
  if (row.action === "Указать себестоимость") return "Укажите себестоимость товара на странице «Себестоимость», чтобы получить точный расчёт прибыли с каждой продажи.";
  if (row.action === "Проверить цену и расходы") return `Товар в минусе: суммарная прибыль ${money(row.profit)}. Пересмотрите цену или сократите расходы WB.`;
  if (row.action === "Поднять цену") return `Маржа ${percent(row.margin)} — ниже целевых 15%. Поднимите цену или снизьте себестоимость.`;
  if (row.action === "Проверить логистику") return "Расходы WB превышают 50% от выручки. Проверьте логистику, хранение и комиссии по товару.";
  if (row.action === "Контролировать остатки") return `Товар прибыльный: ${money(row.profit_per_unit)} с 1 шт., маржа ${percent(row.margin)}. Следите за остатками и рекламным бюджетом.`;
  if (row.action === "Проверить списания WB") return "WB списал расходы без продаж. Проверьте штрафы, удержания и логистику в личном кабинете.";
  return "Проверьте показатели товара.";
}

function PulseTable({ rows, totalRows, query, onQueryChange, reload }) {
  return (
    <section className="panel pulse-panel">
      <div className="panel-head">
        <div>
          <h2>Рука на пульсе</h2>
          <p>Товары, прибыльность, точность данных WB и действие на сегодня.</p>
        </div>
        <Database size={22} />
      </div>
      <div className="pulse-toolbar">
        <label className="search-box pulse-search">
          <Search size={18} />
          <input
            placeholder="Быстрый поиск в таблице..."
            value={query}
            onChange={(event) => onQueryChange(event.target.value)}
          />
        </label>
        <span>{rows.length} из {totalRows} товаров</span>
      </div>
      <div className="table-wrap">
        <table className="pulse-table">
          <thead>
            <tr>
              {["Артикул", "Товар / nmID", "Продано", "Сумма продаж", "До СПП", "СПП", "После СПП", "Себестоимость", "Комиссия WB", "Логистика", "Хранение", "Возвраты", "Эквайринг", "СПА", "Реклама", "Налог", "Прибыль", "Маржа", "ДРР", "Остаток", "Дней хватит", "Статус", "Действие"].map((head) => <th key={head}>{head}</th>)}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => <PulseRow key={row.nm_id} row={row} reload={reload} />)}
            {!rows.length && <tr><td colSpan="23" className="empty">{totalRows ? "По выбранным фильтрам товары не найдены." : "После синхронизации WB здесь появятся товары."}</td></tr>}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function PulseRow({ row, reload }) {
  const [editing, setEditing] = useState(false);
  const [detailsOpen, setDetailsOpen] = useState(false);
  const [saving, setSaving] = useState(false);
  const [rowError, setRowError] = useState("");
  const [cost, setCost] = useState(normalizeCostValue(row.unit_cost_price));

  useEffect(() => {
    setCost(normalizeCostValue(row.unit_cost_price));
  }, [row.unit_cost_price]);

  async function save() {
    const parsed = parseCost(cost);
    if (!parsed.ok) {
      setRowError(parsed.message);
      return;
    }
    setSaving(true);
    setRowError("");
    try {
      await api(`/products/${row.nm_id}/cost-price`, {
        method: "PATCH",
        body: JSON.stringify({ cost_price: parsed.value, vendor_code: row.vendor_code, name: row.name }),
      });
      setEditing(false);
      await reload();
    } catch (err) {
      setRowError(err.message);
    } finally {
      setSaving(false);
    }
  }

  function runAction() {
    if (row.action === "Указать себестоимость") {
      setEditing(true);
      setDetailsOpen(false);
      return;
    }
    setDetailsOpen((current) => !current);
  }

  return (
    <>
      <tr>
        <td>{row.vendor_code || "—"}</td>
        <td className="product-cell"><strong>{row.name}</strong><small>nmID {row.nm_id}</small></td>
        <td>{number(row.sold_qty)}</td>
        <td>{money(row.sales_sum)}</td>
        <td>{money(row.before_spp)}</td>
        <td>{money(row.spp)}</td>
        <td>{money(row.after_spp)}</td>
        <td>
          {editing ? (
            <div className="inline-cost row-cost-editor">
              <input
                className="mini"
                type="number"
                min="0"
                step="0.01"
                value={cost}
                onChange={(event) => setCost(event.target.value)}
                autoFocus
              />
              <button type="button" className="small primary" onClick={save} disabled={saving}><Save size={14} /></button>
              <button type="button" className="small" onClick={() => { setEditing(false); setRowError(""); setCost(normalizeCostValue(row.unit_cost_price)); }}>Отмена</button>
            </div>
          ) : money(row.unit_cost_price)}
          {rowError && <small className="row-error">{rowError}</small>}
        </td>
        <td>{money(row.commission)}</td>
        <td>{money(row.logistics)}</td>
        <td>{money(row.storage)}</td>
        <td>{money(row.returns)}</td>
        <td>{money(row.acquiring)}</td>
        <td>{money(row.spa)}</td>
        <td>{money(row.advertising)}</td>
        <td>{money(row.tax)}</td>
        <td>{money(row.profit)}</td>
        <td>{percent(row.margin)}</td>
        <td>{percent(row.drr)}</td>
        <td>{number(row.stock)}</td>
        <td>{number(row.days_left)}</td>
        <td><span className={`status ${statusClass(row.status)}`}>{row.status}</span></td>
        <td><button type="button" className="small" onClick={runAction}>{detailsOpen ? "Скрыть" : row.action}</button></td>
      </tr>
      {detailsOpen && (
        <tr className="detail-row">
          <td colSpan="23">
            <div className="row-detail">
              <strong>{row.action}</strong>
              <span>{actionHint(row)}</span>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

function statusClass(status) {
  if (status === "В плюсе") return "ok";
  if (["В минусе", "Минус с каждой продажи", "Расходы без продаж", "Нет данных WB", "Нет данных"].includes(status)) return "bad";
  if (status === "Нет продаж") return "neutral";
  return "warn";
}

function actionHint(row) {
  if (row.action === "Проверить карточку товара") {
    const missing = (row.missing_expense_fields || []).map((field) => EXPENSE_LABELS[field] || field).join(", ");
    return missing ? `WB не вернул часть расходов: ${missing}. Проверьте права токена и повторите синхронизацию.` : "Проверьте права токена и повторите синхронизацию WB.";
  }
  if (row.action === "Пополнить остаток") return `Остатка хватит примерно на ${number(row.days_left)} дней. Проверьте поставку и остатки по nmID ${row.nm_id}.`;
  if (row.action === "Поднять цену") return "Товар уходит в минус. Сначала проверьте себестоимость и расходы WB, затем пересмотрите цену.";
  if (row.action === "Снизить расходы") return "Маржа ниже целевого уровня. Проверьте логистику, рекламу, хранение и комиссии по товару.";
  if (row.action === "Проверить рекламу") return "Товар прибыльный. Сравните ДРР с маржей и скорректируйте рекламный бюджет.";
  return "Проверьте показатели товара и обновите данные после изменений.";
}

function nullableNumber(value, fallback) {
  return value === null || value === undefined ? fallback : Number(value);
}

/* ── Landing Page ── */
function LandingPage({ onLogin }) {
  return (
    <div className="landing">
      <header className="landing-header">
        <BrandMark />
        <div className="landing-header-actions">
          <button onClick={onLogin}>Войти</button>
          <button className="primary" onClick={onLogin}>Начать бесплатно</button>
        </div>
      </header>

      <section className="landing-hero">
        <div className="landing-hero-text">
          <span className="soft-label"><Sparkles size={14} /> 14 дней бесплатно</span>
          <h1>Аналитика Wildberries без ручных таблиц</h1>
          <p>SellerPulse считает вашу чистую прибыль, расходы WB и показывает товары, которые требуют внимания — всё в одном кабинете.</p>
          <div className="landing-hero-actions">
            <button className="primary landing-cta" onClick={onLogin}><Sparkles size={17} /> Начать бесплатно — 14 дней</button>
            <button onClick={onLogin}>Войти в кабинет</button>
          </div>
        </div>
        <div className="landing-hero-visual">
          <div className="landing-mockup">
            <div className="landing-mockup-header">
              <span className="landing-mockup-dot" /><span className="landing-mockup-dot" /><span className="landing-mockup-dot" />
              <span className="landing-mockup-url">sellerpulse.app / dashboard</span>
            </div>
            <div className="landing-mockup-metrics">
              <div className="landing-mini-metric"><small>Продажи</small><strong>₽ 248 400</strong></div>
              <div className="landing-mini-metric"><small>Прибыль</small><strong style={{color:"#059669"}}>₽ 61 200</strong></div>
              <div className="landing-mini-metric"><small>Маржа</small><strong>24.6%</strong></div>
              <div className="landing-mini-metric"><small>ДРР</small><strong>8.2%</strong></div>
            </div>
            <div className="landing-mockup-bars">
              {[55, 72, 48, 88, 65, 91, 70].map((h, i) => (
                <span key={i} style={{height: `${h}%`}} />
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="landing-features">
        <h2>Всё что нужно продавцу WB</h2>
        <div className="landing-features-grid">
          <LandingFeature icon={TrendingUp} title="Чистая прибыль" text="Считаем выручку минус все расходы WB, себестоимость и налог. Никаких ручных таблиц." color="blue" />
          <LandingFeature icon={Coins} title="Расходы WB" text="Комиссия, логистика, хранение, реклама, штрафы — автоматически через WB API." color="teal" />
          <LandingFeature icon={FileSpreadsheet} title="Финансовые отчёты" text="Загрузите Excel-отчёт WB для точных цифр — система сама разберёт и посчитает." color="purple" />
          <LandingFeature icon={WalletCards} title="Себестоимость" text="Укажите закупочную цену один раз — система будет считать реальную маржу по каждому товару." color="green" />
          <LandingFeature icon={BarChart3} title="Таблица товаров" text="Все ваши SKU с продажами, маржой, остатками и статусом. Фильтры и сортировка." color="orange" />
          <LandingFeature icon={ShieldCheck} title="Безопасно" text="WB API-токен хранится в зашифрованном виде. Мы не имеем доступ к вашим данным WB." color="red" />
        </div>
      </section>

      <section className="landing-pricing">
        <h2>Простая цена</h2>
        <p className="landing-pricing-sub">Без скрытых платежей. Один тариф — все функции.</p>
        <div className="landing-price-card">
          <div className="landing-price-badge">Самый популярный</div>
          <div className="landing-price-amount"><strong>990 ₽</strong><span> / месяц</span></div>
          <ul className="landing-price-list">
            <li><CheckCircle2 size={16} /> Dashboard с чистой прибылью</li>
            <li><CheckCircle2 size={16} /> Расходы WB автоматически</li>
            <li><CheckCircle2 size={16} /> Финансовые отчёты WB</li>
            <li><CheckCircle2 size={16} /> Таблица всех товаров</li>
            <li><CheckCircle2 size={16} /> Себестоимость по SKU</li>
            <li><CheckCircle2 size={16} /> Неограниченная синхронизация</li>
          </ul>
          <button className="primary wide landing-cta" onClick={onLogin}><Sparkles size={16} /> Попробовать 14 дней бесплатно</button>
          <p className="landing-price-note">Карта не нужна для пробного периода</p>
        </div>
      </section>

      <footer className="landing-footer">
        <BrandMark />
        <p>© 2025 SellerPulse. Аналитика для продавцов Wildberries.</p>
        <div className="landing-footer-links">
          <span>Поддержка: поддержка@sellerpulse.app</span>
        </div>
      </footer>
    </div>
  );
}

function LandingFeature({ icon: Icon, title, text, color }) {
  return (
    <div className={`landing-feature landing-feature--${color}`}>
      <div className="landing-feature-icon"><Icon size={22} /></div>
      <div>
        <strong>{title}</strong>
        <p>{text}</p>
      </div>
    </div>
  );
}

/* ── Subscription Page ── */
function SubscriptionPage({ user, onLogout, onActivated }) {
  const [sub, setSub] = useState(user.subscription || null);

  useEffect(() => {
    api("/subscription").then(setSub).catch(() => {});
  }, []);

  const isTrial = sub?.status === "trial";
  const isExpired = !sub?.active;

  return (
    <div className="sub-page">
      <header className="sub-page-header">
        <BrandMark />
        <button onClick={onLogout}><LogOut size={16} /> Выйти</button>
      </header>
      <div className="sub-page-content">
        <div className="sub-card">
          {isExpired ? (
            <>
              <div className="sub-icon sub-icon--warn"><AlertTriangle size={28} /></div>
              <h2>{isTrial ? "Пробный период истёк" : "Подписка истекла"}</h2>
              <p>Для продолжения работы оформите подписку. Ваши данные сохранены.</p>
            </>
          ) : (
            <>
              <div className="sub-icon sub-icon--ok"><CheckCircle2 size={28} /></div>
              <h2>{isTrial ? `Пробный период — осталось ${sub?.days_left ?? "?"} дн.` : `Подписка активна — ${sub?.days_left ?? "?"} дн.`}</h2>
              <p>{isTrial ? "После пробного периода потребуется оплата для продолжения." : `Активна до ${sub?.paid_until ? new Date(sub.paid_until).toLocaleDateString("ru") : "—"}.`}</p>
              <button className="primary" onClick={onActivated}><Gauge size={16} /> Перейти в кабинет</button>
            </>
          )}

          {isExpired && (
            <div className="sub-payment-block">
              <div className="notice warning" style={{marginBottom: 16}}>
                <AlertTriangle size={18} />
                <div>
                  <strong>Нужна оплата</strong>
                  <span>Напишите нам для оформления подписки — мы пришлём ссылку на оплату.</span>
                </div>
              </div>
              <div className="sub-contact-options">
                <a className="sub-contact-btn sub-contact-btn--tg" href="https://t.me/sellerpulse_support" target="_blank" rel="noreferrer">
                  <strong>Написать в Telegram</strong>
                  <span>Быстрый ответ</span>
                </a>
                <a className="sub-contact-btn" href="mailto:support@sellerpulse.app">
                  <strong>Написать на Email</strong>
                  <span>support@sellerpulse.app</span>
                </a>
              </div>
            </div>
          )}

          <div className="sub-price-info">
            <strong>990 ₽ / месяц</strong>
            <span>Все функции включены</span>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ── Admin Page ── */
const SUB_STATUS_LABEL = { trial: "Пробный", active: "Активна", expired: "Истекла", cancelled: "Отменена", no_subscription: "Нет" };
const SUB_STATUS_CLASS = { trial: "warn", active: "ok", expired: "bad", cancelled: "bad", no_subscription: "bad" };

function AdminPage({ user, onLogout, onNavigate }) {
  const [stats, setStats] = useState(null);
  const [users, setUsers] = useState([]);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [patching, setPatching] = useState({});
  const [message, setMessage] = useState("");
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState("all");
  const [expiringFilter, setExpiringFilter] = useState("all");
  const [selectedIds, setSelectedIds] = useState([]);
  const [bulkDays, setBulkDays] = useState("30");
  const [bulkNotes, setBulkNotes] = useState("");
  const [bulkLoading, setBulkLoading] = useState(false);
  const [customDays, setCustomDays] = useState({});
  const [customNotes, setCustomNotes] = useState({});

  useEffect(() => { load(); }, []);

  async function load() {
    setLoading(true); setError("");
    try {
      const [usersData, statsData] = await Promise.all([api("/admin/users"), api("/admin/stats")]);
      setUsers(usersData);
      setStats(statsData);
    } catch (err) { setError(err.message); }
    finally { setLoading(false); }
  }

  async function patchSub(userId, status, days, notes) {
    setPatching((p) => ({ ...p, [userId]: true }));
    setMessage(""); setError("");
    try {
      const payload = { status, days: days || null };
      if (notes !== undefined) payload.notes = notes;
      await api(`/admin/users/${userId}/subscription`, {
        method: "PATCH",
        body: JSON.stringify(payload),
      });
      setMessage(`✓ Подписка пользователя #${userId} обновлена`);
      load();
    } catch (err) { setError(err.message); }
    finally { setPatching((p) => ({ ...p, [userId]: false })); }
  }

  async function patchBulk(status, days, notes) {
    if (!selectedIds.length) return;
    setBulkLoading(true);
    setMessage(""); setError("");
    try {
      const payload = { user_ids: selectedIds, status, days: days || null };
      if (notes !== undefined) payload.notes = notes;
      const result = await api("/admin/subscriptions/bulk", {
        method: "PATCH",
        body: JSON.stringify(payload),
      });
      setMessage(`✓ Обновлено пользователей: ${result.count}`);
      setSelectedIds([]);
      load();
    } catch (err) { setError(err.message); }
    finally { setBulkLoading(false); }
  }

  function isExpiringMatch(sub) {
    if (expiringFilter === "all") return true;
    const daysLeft = sub.days_left;
    if (daysLeft == null || !sub.active) return false;
    if (expiringFilter === "today") return daysLeft === 0;
    if (expiringFilter === "3") return daysLeft <= 3;
    if (expiringFilter === "7") return daysLeft <= 7;
    return true;
  }

  function handleBlock(userId) {
    if (!window.confirm("Заблокировать подписку пользователя?")) return;
    patchSub(userId, "expired", 0);
  }

  function handleBulkBlock() {
    if (!selectedIds.length) return;
    if (!window.confirm(`Заблокировать подписку для ${selectedIds.length} пользователей?`)) return;
    patchBulk("expired", 0);
  }

  function toggleUserSelection(userId) {
    setSelectedIds((current) => (
      current.includes(userId) ? current.filter((id) => id !== userId) : [...current, userId]
    ));
  }

  const filtered = users.filter((u) => {
    const matchStatus = statusFilter === "all" || u.subscription.status === statusFilter;
    const q = query.toLowerCase();
    const matchQuery = !q || u.name.toLowerCase().includes(q) || u.email.toLowerCase().includes(q) || String(u.id).includes(q);
    return matchStatus && matchQuery && isExpiringMatch(u.subscription);
  });

  const allFilteredSelected = filtered.length > 0 && filtered.every((u) => selectedIds.includes(u.id));

  function toggleSelectAllFiltered() {
    if (allFilteredSelected) {
      setSelectedIds((current) => current.filter((id) => !filtered.some((u) => u.id === id)));
      return;
    }
    setSelectedIds((current) => Array.from(new Set([...current, ...filtered.map((u) => u.id)])));
  }

  return (
    <AppShell user={user} active="admin" onLogout={onLogout} onNavigate={onNavigate} shop={null}>
      <section className="page-title">
        <div>
          <p className="eyebrow">Только для администратора</p>
          <h1>Мониторинг тарифов</h1>
          <p>Статистика подписок, пользователи и управление тарифами.</p>
        </div>
        <button onClick={load} disabled={loading}><RefreshCcw size={16} className={loading ? "spin" : ""} /> Обновить</button>
      </section>

      {error && <div className="notice warning"><AlertTriangle size={18} /> {error}</div>}

      {/* Статистика */}
      {stats && (
        <>
          <div className="admin-stats-grid">
            <div className="admin-stat admin-stat--blue">
              <div className="admin-stat-icon"><UserRound size={20} /></div>
              <div>
                <strong>{stats.total_users}</strong>
                <span>Всего пользователей</span>
              </div>
              <div className="admin-stat-sub">+{stats.new_today} сегодня · +{stats.new_week} за 7 дн.</div>
            </div>
            <div className="admin-stat admin-stat--green">
              <div className="admin-stat-icon"><CheckCircle2 size={20} /></div>
              <div>
                <strong>{stats.active_paid}</strong>
                <span>Активных подписок</span>
              </div>
              <div className="admin-stat-sub">MRR ≈ {(stats.mrr_estimate).toLocaleString("ru")} ₽</div>
            </div>
            <div className="admin-stat admin-stat--teal">
              <div className="admin-stat-icon"><Sparkles size={20} /></div>
              <div>
                <strong>{stats.active_trial}</strong>
                <span>Пробный период</span>
              </div>
              <div className="admin-stat-sub">Конвертация в оплату</div>
            </div>
            <div className="admin-stat admin-stat--red">
              <div className="admin-stat-icon"><AlertTriangle size={20} /></div>
              <div>
                <strong>{stats.expired}</strong>
                <span>Истекло / заблок.</span>
              </div>
              <div className="admin-stat-sub">Нет подписки: {stats.no_subscription}</div>
            </div>
          </div>

          {/* Истекают скоро */}
          {stats.expiring_soon.length > 0 && (
            <div className="panel admin-expiring-panel">
              <div className="panel-head">
                <div>
                  <h2>⚠ Истекают в ближайшие 7 дней</h2>
                  <p>Свяжитесь с пользователями для продления.</p>
                </div>
                <span className="pill warn">{stats.expiring_soon.length}</span>
              </div>
              <div className="admin-expiring-list">
                {stats.expiring_soon.map((u) => (
                  <div key={u.id} className="admin-expiring-row">
                    <div className="admin-expiring-info">
                      <strong>{u.name}</strong>
                      <span>{u.email}</span>
                    </div>
                    <div className="admin-expiring-days">
                      <span className={`pill ${u.days_left <= 2 ? "invalid" : "warn"}`}>{u.days_left === 0 ? "сегодня" : `${u.days_left} дн.`}</span>
                      <small>{new Date(u.end_date).toLocaleDateString("ru")}</small>
                    </div>
                    <button className="small primary" onClick={() => patchSub(u.id, "active", 30)} disabled={patching[u.id]}>
                      +30 дней
                    </button>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Таблица пользователей */}
          <div className="table-card" style={{marginTop:16}}>
            <div className="table-head-row">
              <div>
                <h3>Пользователи</h3>
                <p>Всего: {filtered.length} из {users.length}</p>
              </div>
              <div style={{display:"flex", gap:8, flexWrap:"wrap"}}>
                <label className={`search-box ${query ? "" : ""}`} style={{minWidth:200}}>
                  <Search size={16} />
                  <input placeholder="Поиск по имени, email, ID..." value={query} onChange={(e) => setQuery(e.target.value)} />
                </label>
                <select value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)} style={{minHeight:40,padding:"0 12px",border:"1px solid var(--line)",borderRadius:"var(--r)",background:"#fff",fontWeight:700}}>
                  <option value="all">Все статусы</option>
                  <option value="active">Активные</option>
                  <option value="trial">Пробный</option>
                  <option value="expired">Истекшие</option>
                  <option value="cancelled">Отменённые</option>
                </select>
                <select value={expiringFilter} onChange={(e) => setExpiringFilter(e.target.value)} style={{minHeight:40,padding:"0 12px",border:"1px solid var(--line)",borderRadius:"var(--r)",background:"#fff",fontWeight:700}}>
                  <option value="all">Все сроки</option>
                  <option value="today">Истекают сегодня</option>
                  <option value="3">До 3 дней</option>
                  <option value="7">До 7 дней</option>
                </select>
              </div>
            </div>
            {selectedIds.length > 0 && (
              <div className="admin-bulk-bar">
                <strong>Выбрано: {selectedIds.length}</strong>
                <div className="admin-bulk-controls">
                  <input
                    type="number"
                    className="admin-days-input"
                    min={1}
                    max={365}
                    value={bulkDays}
                    onChange={(e) => setBulkDays(e.target.value)}
                  />
                  <button className="small ghost" onClick={() => patchBulk("active", 7)} disabled={bulkLoading}>+7</button>
                  <button className="small ghost" onClick={() => patchBulk("active", 30)} disabled={bulkLoading}>+30</button>
                  <button className="small ghost" onClick={() => patchBulk("active", 90)} disabled={bulkLoading}>+90</button>
                  <button className="small primary" onClick={() => patchBulk("active", parseInt(bulkDays) || 30, bulkNotes || undefined)} disabled={bulkLoading}>
                    Активировать
                  </button>
                  <button className="small" onClick={() => patchBulk("trial", 14, bulkNotes || undefined)} disabled={bulkLoading}>
                    Пробный 14д
                  </button>
                  <button className="small danger" onClick={handleBulkBlock} disabled={bulkLoading}>
                    Заблок.
                  </button>
                  <button className="small" onClick={() => setSelectedIds([])} disabled={bulkLoading}>
                    Снять выбор
                  </button>
                </div>
                <input
                  type="text"
                  className="admin-notes-input"
                  placeholder="Общая заметка для выбранных пользователей"
                  value={bulkNotes}
                  onChange={(e) => setBulkNotes(e.target.value)}
                />
              </div>
            )}
            {message && <div className="notice info" style={{margin:"12px 22px 0"}}>{message}</div>}
            {error && <div className="error" style={{margin:"12px 22px 0"}}>{error}</div>}
            <div className="table-wrap" style={{maxHeight:600}}>
              <table>
                <thead>
                  <tr>
                    <th style={{width:44}}>
                      <input type="checkbox" checked={allFilteredSelected} onChange={toggleSelectAllFiltered} />
                    </th>
                    <th style={{width:44}}>ID</th>
                    <th>Пользователь</th>
                    <th>Email</th>
                    <th>Зарег.</th>
                    <th>Тариф</th>
                    <th>До / Осталось</th>
                    <th style={{minWidth:360}}>Действия</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((u) => {
                    const sub = u.subscription;
                    const endDate = sub.paid_until || sub.trial_end;
                    return (
                      <tr key={u.id}>
                        <td>
                          <input
                            type="checkbox"
                            checked={selectedIds.includes(u.id)}
                            onChange={() => toggleUserSelection(u.id)}
                          />
                        </td>
                        <td style={{color:"var(--muted)",fontWeight:700}}>{u.id}</td>
                        <td>
                          <div style={{display:"flex",alignItems:"center",gap:8}}>
                            <div className="admin-user-avatar">{u.name[0]?.toUpperCase()}</div>
                            <div>
                              <strong>{u.name}</strong>
                              {u.is_admin && <span className="pill active" style={{marginLeft:6,fontSize:10,padding:"2px 7px"}}>admin</span>}
                            </div>
                          </div>
                        </td>
                        <td style={{color:"var(--muted)"}}>{u.email}</td>
                        <td style={{color:"var(--muted)",fontSize:12}}>{new Date(u.created_at).toLocaleDateString("ru")}</td>
                        <td>
                          <span className={`status ${SUB_STATUS_CLASS[sub.status] || "neutral"}`}>
                            {SUB_STATUS_LABEL[sub.status] || sub.status}
                          </span>
                        </td>
                        <td>
                          <div>
                            {endDate ? <strong style={{fontSize:13}}>{new Date(endDate).toLocaleDateString("ru")}</strong> : <span style={{color:"var(--muted)"}}>—</span>}
                            {sub.days_left != null && <small style={{display:"block",color:"var(--muted)",fontSize:11}}>{sub.days_left} дн. осталось</small>}
                          </div>
                        </td>
                        <td>
                          <div className="admin-actions-row">
                            <input
                              type="number"
                              className="admin-days-input"
                              placeholder="дней"
                              min={1} max={365}
                              value={customDays[u.id] || ""}
                              onChange={(e) => setCustomDays((d) => ({ ...d, [u.id]: e.target.value }))}
                            />
                            {[7, 30, 90].map((days) => (
                              <button
                                key={days}
                                type="button"
                                className="small ghost"
                                onClick={() => {
                                  setCustomDays((d) => ({ ...d, [u.id]: String(days) }));
                                  patchSub(u.id, "active", days);
                                }}
                                disabled={patching[u.id]}
                              >
                                +{days}
                              </button>
                            ))}
                            <button className="small primary" onClick={() => patchSub(u.id, "active", parseInt(customDays[u.id]) || 30)} disabled={patching[u.id]}>
                              Активировать
                            </button>
                            <button className="small" onClick={() => patchSub(u.id, "trial", 14)} disabled={patching[u.id]}>
                              Пробный 14д
                            </button>
                            <button className="small danger" onClick={() => handleBlock(u.id)} disabled={patching[u.id]}>
                              Заблок.
                            </button>
                          </div>
                          <input
                            type="text"
                            className="admin-notes-input"
                            placeholder="Заметка (оплата, канал...)"
                            value={customNotes[u.id] ?? sub.notes ?? ""}
                            onChange={(e) => setCustomNotes((n) => ({ ...n, [u.id]: e.target.value }))}
                            onBlur={(e) => e.target.value !== (sub.notes || "") && patchSub(u.id, sub.status, null, e.target.value)}
                          />
                          {u.subscription_history?.length > 0 && (
                            <details className="admin-history">
                              <summary>История изменений</summary>
                              <div className="admin-history-list">
                                {u.subscription_history.map((item) => (
                                  <div key={item.id} className="admin-history-item">
                                    <strong>{dateTime(item.created_at)}</strong>
                                    <span>
                                      {(item.admin_name || "Админ")} {" -> "} {SUB_STATUS_LABEL[item.new_status] || item.new_status}
                                      {item.days_added ? `, +${item.days_added} дн.` : ""}
                                    </span>
                                    {item.notes && <em>{item.notes}</em>}
                                  </div>
                                ))}
                              </div>
                            </details>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                  {filtered.length === 0 && (
                    <tr><td colSpan={8} className="empty">Нет пользователей по фильтру</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </AppShell>
  );
}

createRoot(document.getElementById("root")).render(<App />);
