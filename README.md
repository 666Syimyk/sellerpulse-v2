# SellerPulse v2

Аналитическая платформа для продавцов Wildberries: регистрация, подключение WB API-токена, себестоимость товаров и Dashboard с продажами, расходами WB, прибылью, маржей, ДРР и рекомендациями.

## Структура

```text
frontend/   React + Vite
backend/    FastAPI
database/   SQL-схема PostgreSQL
```

## Быстрый старт

### Автозапуск на Windows

Откройте файл `запустить`, выделите весь текст и выполните его в PowerShell. Скрипт создаст `.venv`, установит зависимости backend, создаст `.env` из примера и откроет два окна: API на `http://localhost:8000` и frontend на `http://localhost:5173`.

### Backend

```bash
cd backend
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
uvicorn main:app --reload --port 8000
```

По умолчанию `.env.example` настроен на локальную SQLite-базу для быстрой проверки. Для PostgreSQL задайте `DATABASE_URL=postgresql+psycopg://...`.

### Прод-режим: PostgreSQL + отдельный worker

Для SaaS-сценария с подписками и фоновыми синхронизациями запускайте проект через Docker Compose:

```bash
docker compose up --build
```

Будут подняты:

- `db` — PostgreSQL
- `backend` — FastAPI API
- `worker` — отдельный процесс, который забирает `sync_jobs` из БД и выполняет синхронизацию WB
- `frontend` — nginx со сборкой React

В этой схеме:

- web-процесс больше не выполняет долгие WB sync внутри HTTP-запроса;
- планировщик только ставит `auto_sync` jobs в очередь;
- worker отдельно обрабатывает `manual_sync`, `initial_full_sync` и `auto_sync`.

### Frontend

```bash
cd frontend
npm install
npm run dev
```

Откройте `http://localhost:5173`.

Проверка frontend:

```bash
cd frontend
npm run test
npm run check
```

## Что реализовано

- 3 основных экрана: вход/регистрация, подключение WB API-токена, Dashboard.
- Проверка WB-токена через JWT bitmask прав и `/ping`.
- Статусы токена: `active`, `limited`, `invalid`, `rate_limited`.
- Расчёт прибыли по заданной формуле.
- Отдельное состояние `Нет данных WB`, без подмены отсутствующих расходов нулями.
- Таблица “Рука на пульсе” с себестоимостью, точностью расчёта, статусами и действиями.
- Загрузка финансового отчёта WB из Excel/CSV/ZIP с расчётом прибыли по товарам.
- Сервис синхронизации WB с сохранением последних данных при rate limit.
