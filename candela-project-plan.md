# Candela — Project Plan

## Overview

Candela is a standalone solar analytics and electricity plan comparison app. It collects real-time data from a Sungrow SG5.0RS-ADA inverter, stores it at 5-minute resolution, and lets you model your actual electricity costs against multiple tariff structures: single rate, time-of-use (TOU), demand, and AEMO wholesale (as a proxy for Amber Electric spot pricing). It also performs load disaggregation to identify EV charging and hot water heat pump events within the consumption data.

**Stack:**
- Python 3.14
- FastAPI + HTMX (Jinja2 templates)
- PostgreSQL (separate `candela` database, same instance as Glow-worm)
- Alembic for migrations (same pattern as Glow-worm)
- `uv` for dependency management
- Podman Quadlet on Fedora CoreOS NUC

**Not integrated with Glow-worm at this stage.** Integration is a future phase documented at the end of this plan.

---

## Hardware Context

- **Inverter:** Sungrow SG5.0RS-ADA (5kW string inverter, single-phase)
- **Comms:** Built-in WiNet-S2 Wi-Fi module (no external dongle)
- **Hot water:** Midea RSJ-15/190RDN3-E heat pump (190L, 1500W rated, 2780W boost element)
- **EV:** Tesla (charging inferred from inverter load data only)
- **Network:** Inverter accessible on LAN via WiNet-S2 IP
- **Distributor:** Energex (South East Queensland)
- **Retailer:** Engie

---

## Project Structure

```
candela/
├── candela/
│   ├── main.py
│   ├── config.py              # pydantic-settings, reads from env
│   ├── db.py                  # asyncpg pool setup
│   ├── collector/
│   │   ├── inverter.py        # WiNet-S2 client wrapper
│   │   ├── poller.py          # APScheduler polling loop
│   │   ├── backfill.py        # iSolarCloud CSV import
│   │   └── aemo.py            # AEMO wholesale price fetcher
│   ├── tariffs/
│   │   ├── engine.py          # compute_bill() entry point
│   │   ├── strategies/
│   │   │   ├── base.py        # TariffStrategy protocol
│   │   │   ├── single_rate.py
│   │   │   ├── tou.py
│   │   │   ├── demand.py
│   │   │   └── wholesale.py   # AEMO-based Amber proxy
│   │   └── seed.py            # seed known plans into DB
│   ├── disaggregation/
│   │   ├── detector.py        # sliding window load detection
│   │   └── reconciler.py      # manual confirmation tracking
│   ├── api/
│   │   ├── readings.py
│   │   ├── tariffs.py
│   │   ├── loads.py
│   │   └── summary.py
│   └── web/
│       ├── templates/
│       └── static/
├── migrations/
├── tests/
├── pyproject.toml
└── Containerfile
```

---

## Phase 0 — Project Scaffolding

**Goal:** Repo structure, tooling, database, and dev environment ready before any feature work begins.

### Steps

1. **Initialise repo with `uv` and Python 3.14.**

2. **Add dependencies:**
   - `fastapi`, `uvicorn[standard]`
   - `asyncpg`
   - `alembic`
   - `pydantic-settings`
   - `jinja2`, `python-multipart`
   - `httpx`
   - `apscheduler`
   - `sungrow-websocket`
   - `aiosqlite` (SQLite async driver for development)
   - `pytest`, `pytest-asyncio`, `coverage`

3. **Create `candela` database** in the existing PostgreSQL instance. Do not share schemas with Glow-worm. Alembic config points to `candela` DB only.

4. **Mirror Glow-worm's Alembic setup:** same `alembic.ini` structure, separate `migrations/` folder, same env-based `DATABASE_URL` pattern.

5. **Database abstraction layer** in `db.py` — detect backend from `DATABASE_URL` scheme and initialise the appropriate async driver:
   - `postgresql+asyncpg://` → `asyncpg` connection pool (production)
   - `sqlite+aiosqlite://` → `aiosqlite` connection (development)
   
   All SQL in the codebase must be compatible with both backends. Avoid Postgres-specific syntax (e.g. use `CURRENT_TIMESTAMP` not `NOW()`, avoid `RETURNING` clauses where possible, use `INTEGER` not `SERIAL` in shared migration files — use Alembic's `server_default` and `autoincrement` abstractions). Alembic `env.py` reads `DATABASE_URL` and configures the correct dialect automatically.

   Development workflow: `cp .env.example .env.dev` and set `DATABASE_URL=sqlite+aiosqlite:///./candela.db`. Run `uv run alembic upgrade head` to initialise the local SQLite DB. No Postgres instance required for local development.

5. **Configure `pydantic-settings`** in `config.py`. Required env vars:

   | Variable | Description |
   |---|---|
   | `DATABASE_URL` | DB connection URL. Postgres (`postgresql+asyncpg://...`) for production; SQLite (`sqlite+aiosqlite:///./candela.db`) for development |
   | `INVERTER_HOST` | IP address of WiNet-S2 on LAN |
   | `INVERTER_POLL_INTERVAL_SECONDS` | Default: `300` |
   | `AEMO_REGION` | Default: `QLD1` |
   | `WHOLESALE_ADDER_CENTS_KWH` | Network + env + retail stack estimate. Default: `18.0` |

6. **Two Podman Quadlet units:**
   - `candela-web.container` — FastAPI app via Uvicorn
   - `candela-collector.container` — long-running poller process
   
   Keeping them separate means the web UI can be restarted independently of data collection, and the collector won't drop readings during a web deploy.

7. **Containerfile:** single image used by both units, entrypoint differs via `CMD` override in each Quadlet file.

---

## Phase 1 — Data Collection

**Goal:** Reliable 5-minute inverter data flowing into the database (Postgres in production, SQLite in development). Stable enough to leave running unattended.

### Database Schema

```sql
CREATE TABLE solar_readings (
    ts                  TEXT        NOT NULL,  -- ISO8601 UTC; Alembic maps to TIMESTAMPTZ on Postgres
    solar_w             INTEGER     NOT NULL,  -- PV generation watts
    grid_w              INTEGER     NOT NULL,  -- positive=import, negative=export
    load_w              INTEGER     NOT NULL,  -- house consumption watts
    daily_yield_kwh     REAL,
    total_yield_kwh     REAL,
    inverter_temp_c     REAL,
    PRIMARY KEY (ts)
);

CREATE INDEX solar_readings_ts_idx ON solar_readings (ts DESC);
```

> **SQLite compatibility note:** Use Alembic's `sa.DateTime(timezone=True)` column type — asyncpg renders this as `TIMESTAMPTZ` on Postgres and `TEXT` on SQLite. Always store and query timestamps as UTC ISO8601 strings in application code. Use `NUMERIC` → `REAL` similarly. This convention applies to all tables.

### Steps

1. **`collector/inverter.py`** — thin wrapper around `sungrow-websocket`. Returns a typed `InverterReading` dataclass with the fields above. Handles the case where the inverter is offline overnight (returns `None`, does not raise).

2. **`collector/poller.py`** — APScheduler blocking scheduler running in the collector container. Polls every `INVERTER_POLL_INTERVAL_SECONDS`. On success: upsert to `solar_readings`. On connection failure: log warning, skip interval, do not crash. On 3 consecutive failures: log error.

3. **WiNet-S2 stability note:** Do not poll faster than 30 seconds. The built-in WiNet-S2 module on the SG5.0RS-ADA is known to drop offline under aggressive polling. Default 300s (5 min) is conservative and appropriate for this use case.

4. **`collector/backfill.py`** — one-shot CLI script to import historical data from an iSolarCloud CSV export. The iSolarCloud app allows exporting per-day energy data. This seeds initial weeks of data before live polling begins. Run once manually: `uv run python -m candela.collector.backfill --file export.csv`.

5. **`collector/aemo.py`** — fetches AEMO `TRADINGPRICE` CSV zip files for `QLD1` region. Stores results in `aemo_trading_prices` (see Phase 2). Runs as a daily scheduled job (AEMO publishes historical files with a short lag, so fetch previous day's data each morning).

   ```
   Base URL pattern:
   https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/
     {year}/MMSDM_{year}_{month}/MMSDM_Historical_Data_SQLLoader/DATA/
     PUBLIC_DVD_TRADINGPRICE_{year}{month}010000.zip
   ```

   AEMO CSV format note: files contain multiple interleaved record types. Filter rows where the first column (`I`) is `D` (data rows) and `REGIONID == "QLD1"`. Price field is `RRP` in $/MWh. Timestamp field is `SETTLEMENTDATE` (end of 30-min interval — shift back 30 min to get interval start).

6. **Tests:**
   - Mock `sungrow-websocket` client, assert correct DB writes
   - Assert poller skips gracefully on connection error without crashing
   - Assert backfill correctly parses iSolarCloud CSV format
   - Assert AEMO fetcher correctly filters QLD1 rows and parses RRP

---

## Phase 2 — Tariff Engine

**Goal:** Given a date range of `solar_readings`, compute the electricity cost under any configured tariff plan. Pure, testable, no side effects.

### Database Schema

```sql
CREATE TABLE tariff_plans (
    id                          SERIAL PRIMARY KEY,
    name                        TEXT        NOT NULL,
    retailer                    TEXT,
    plan_type                   TEXT        NOT NULL,
    -- plan_type: 'single_rate' | 'tou' | 'demand' | 'wholesale'
    supply_charge_daily_cents   NUMERIC(8, 4) NOT NULL,
    feed_in_tariff_cents        NUMERIC(8, 4),
    valid_from                  DATE        NOT NULL,
    valid_to                    DATE,
    notes                       TEXT
);

CREATE TABLE tariff_rates (
    id                  SERIAL PRIMARY KEY,
    plan_id             INTEGER     NOT NULL REFERENCES tariff_plans(id) ON DELETE CASCADE,
    rate_type           TEXT        NOT NULL,
    -- rate_type: 'flat' | 'peak' | 'shoulder' | 'offpeak' | 'demand'
    cents_per_kwh       NUMERIC(8, 4),   -- NULL for demand charge rows
    cents_per_kw        NUMERIC(8, 4),   -- NULL for energy charge rows
    window_start        TIME,            -- NULL = applies all times
    window_end          TIME,            -- NULL = applies all times
    days_of_week        INTEGER[],       -- 0=Mon..6=Sun, NULL=all days
    months              INTEGER[],       -- 1-12, NULL=all months
    demand_window_start TIME,            -- peak window for demand measurement
    demand_window_end   TIME             -- peak window for demand measurement
);

CREATE TABLE aemo_trading_prices (
    interval_start      TIMESTAMPTZ NOT NULL,
    interval_end        TIMESTAMPTZ NOT NULL,
    rrp_per_mwh         NUMERIC(10, 4) NOT NULL,  -- can be negative
    region              TEXT        NOT NULL DEFAULT 'QLD1',
    PRIMARY KEY (interval_start, region)
);
```

### Tariff Strategies

All strategies implement a common `TariffStrategy` protocol:

```python
class TariffStrategy(Protocol):
    def compute(
        self,
        readings: list[SolarReading],
        plan: TariffPlan,
        rates: list[TariffRate],
    ) -> BillResult: ...
```

`BillResult` contains:
- `total_cents` — bottom line
- `supply_charge_cents`
- `import_charge_cents` — broken down by period
- `export_credit_cents`
- `demand_charge_cents` — zero for non-demand plans
- `period_breakdown` — dict of period name → kWh and cents

**Single rate:**
Sum all import kWh × flat rate. Supply charge × days. Subtract export kWh × FiT.

**TOU:**
Split import readings into period buckets using `window_start`/`window_end` and `days_of_week`. Apply the matching rate to each bucket. Periods are matched in specificity order (peak before shoulder before offpeak). Any unmatched interval falls to offpeak.

**Demand:**
TOU energy charges as above, plus a demand charge. Per Energex's definition: demand is the average kW over a 30-minute interval, calculated as the kWh consumed in that interval multiplied by 2 (i.e. kWh ÷ 0.5 hours = kW average). The demand charge for the billing period is based on the single highest such value recorded during the demand window.

With 5-minute polling, group readings into 30-minute blocks aligned to the clock (e.g. 16:00–16:30, 16:30–17:00). For each block, sum the energy consumed across the constituent readings, then multiply by 2 to get average kW:

```python
# Energex demand calculation per 30-min block
# Source: https://www.energex.com.au/.../residential-tariffs
# "A simple way to calculate demand over a 30-minute period is to
#  multiply the kWh consumption by two."
block_kwh = sum(
    max(reading.grid_w, 0) * (interval_minutes / 60) / 1000
    for reading in block_readings
)
block_demand_kw = block_kwh * 2  # equivalent to kWh / 0.5 hours
```

The demand charge for the month = `max(block_demand_kw across all blocks in demand window)` × `cents_per_kw`.

Energex demand tariff specifics (as of July 2025):
- Peak demand window: 4pm–9pm, all days
- Demand charge: based on highest single 30-min block in the billing period (monthly reset for Engie residential customers)

**Wholesale (AEMO proxy):**
Join each `solar_reading` to `aemo_trading_prices` by nearest 30-minute interval. Import cost = `grid_w` (when positive) × duration × (`rrp_per_mwh` / 1000 / 100 + `wholesale_adder_cents_per_kwh`). Export credit = `grid_w` (when negative) × duration × (`rrp_per_mwh` × 0.7 / 1000 / 100) — the 0.7 factor approximates the discount on feed-in vs import wholesale price. Supply charge applies as normal.

### Steps

1. **`tariffs/engine.py`** — `compute_bill(plan_id, date_from, date_to) -> BillResult`. Fetches readings and plan config from DB, selects correct strategy, delegates computation.

2. **Implement the four strategy classes** in `tariffs/strategies/`.

3. **`tariffs/seed.py`** — seeds the following plans on first run:

   | Plan | Type | Notes |
   |---|---|---|
   | Engie Single Rate (current) | `single_rate` | Enter rates from your current bill |
   | Energex TOU (standard) | `tou` | Peak 4–9pm, shoulder 7am–4pm + 9pm–11pm, offpeak overnight |
   | Energex TOU + Demand | `demand` | As above + demand charge, 4–9pm window |
   | AEMO Wholesale (QLD1) | `wholesale` | Proxy for Amber, `wholesale_adder_cents_per_kwh` configurable |

4. **Tests:** Use synthetic `solar_readings` fixtures with known patterns. Assert bill totals match hand-calculated expected values for each strategy. Demand test must include a deliberate spike to verify correct identification of the peak 30-min block. Wholesale test must include a negative price interval to verify export credit logic.

---

## Phase 3 — Load Disaggregation

**Goal:** Identify EV charging and hot water heat pump events within the `load_w` time series, using inverter load data only. All detection is inference-based; users can manually confirm or reject events via the UI.

### Database Schema

```sql
CREATE TABLE load_events (
    id              SERIAL      PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ,
    load_name       TEXT        NOT NULL,
    -- load_name: 'ev_charging' | 'hot_water_heatpump' | 'hot_water_boost' | 'unknown'
    avg_watts       INTEGER,
    kwh             NUMERIC(8, 3),
    confidence      NUMERIC(4, 3),   -- 0.000 to 1.000
    source          TEXT        NOT NULL
    -- source: 'inferred' | 'tesla_api' | 'manual'
);
```

### Known Load Profiles

```python
KNOWN_LOADS = [
    {
        "name": "ev_charging",
        "min_watts": 1800,
        "max_watts": 8000,      # covers 10A outlet (~2.4kW) and Wall Connector (~7.2kW)
        "tolerance": 500,
        "min_duration_min": 20,
        # confirm_source: "tesla_api" — reserved for future Tesla Fleet API integration
    },
    {
        "name": "hot_water_heatpump",
        # Midea RSJ-15/190RDN3-E: 1500W rated, real-world 800–1400W at Brisbane ambient
        "min_watts": 700,
        "max_watts": 1600,
        "tolerance": 300,
        "min_duration_min": 45,  # 190L tank takes 2–3 hours from cold
    },
    {
        "name": "hot_water_boost",
        # Midea boost element: 2780W rated
        "min_watts": 2500,
        "max_watts": 3000,
        "tolerance": 250,
        "min_duration_min": 15,
    },
]
```

### Detection Approach

Rule-based sliding window over `solar_readings`. For each reading:

1. Compute `delta_w = load_w - previous_load_w`
2. If `abs(delta_w)` exceeds a step threshold (~400W), treat as a candidate event boundary
3. Check whether the new `load_w` level matches any known load profile (within tolerance)
4. If the elevated load is sustained for `min_duration_min`, record as a `load_event` with `source='inferred'`
5. When a load drops back to baseline, close the event and calculate `kwh`

**Overlap handling:** If EV charging and hot water are running simultaneously, the combined `load_w` will exceed any single profile. Detect the larger load (EV) first, subtract its expected wattage from `load_w`, then check the residual against smaller profiles.

**Baseline:** Calculate rolling median of `load_w` during overnight hours (11pm–6am, excluding detected events) as the household idle baseline. Typically 200–500W.

### Steps

1. **`disaggregation/detector.py`** — implements the sliding window logic above. Runs as a daily scheduled job over the prior day's readings rather than in real-time. Upserts detected events to `load_events`.

2. **`disaggregation/reconciler.py`** — provides confidence scoring based on internal consistency checks (e.g. does the detected EV event duration correlate with typical charge times? Does it appear at a consistent time of day?). Does not use any external API. Users can manually confirm or reject events via the UI, which updates the `source` field to `'manual'` and anchors the confidence to `1.0` or `0.0`.

3. **Tuning workflow:** After 2–4 weeks of data, review the load events page for false positives and missed events. Adjust `KNOWN_LOADS` thresholds in config accordingly. This is a deliberate manual tuning step — not automated ML.

4. **Future:** Tesla Fleet API integration is a future phase. When added, it will write `source='tesla_api'` events alongside inferred ones, and the reconciler will use them as ground truth to auto-tune confidence. The schema already supports this via the `source` field.

5. **Tests:**
   - Synthetic `load_w` series with planted step changes, assert correct event detection
   - Overlap test: EV + hot water simultaneously, assert both detected
   - Reconciler test: known inferred events, assert confidence scoring logic
   - Assert manual confirm/reject updates propagate correctly

---

## Phase 4 — Web UI

**Goal:** HTMX-driven UI to view solar data, manage tariff plans, compare costs, and review load events. No frontend build step.

### Design Principles

- Jinja2 templates, HTMX for all interactivity
- Pico CSS (classless, no build step, looks clean out of the box)
- Chart.js via CDN for any visualisations
- All data endpoints are proper JSON API routes — UI uses them via HTMX, but they are also independently usable for future integrations
- Server-side rendering first — HTMX swaps in partials on interaction

### Pages

#### Dashboard (`/`)

- **Current status panel:** last inverter reading (solar W, grid W, load W). Refreshes every 60s via `hx-trigger="every 60s"`.
- **Today's summary:** solar generated kWh, grid imported kWh, exported kWh, self-consumption %, estimated cost today under current plan.
- **Active load indicator:** if a load event is currently inferred as running (EV charging, hot water), show it.
- **Wholesale price strip:** current AEMO QLD1 price in c/kWh (most recent 30-min interval). Colour-coded: green (cheap/negative), amber (moderate), red (high).

#### History (`/history`)

- Date range picker (default: last 30 days).
- Daily bar chart: solar generation vs grid import vs grid export (Chart.js, rendered via HTMX partial on date change).
- Load event timeline below chart: horizontal bars showing inferred EV and hot water events per day.
- Export to CSV button for the selected range.

#### Plan Comparison (`/compare`)

- Date range selector (default: last full calendar month).
- Side-by-side cost cards for each configured plan: total cost, broken down into supply charge / energy charges by period / demand charge / feed-in credit.
- **Demand callout:** for the demand plan, highlight the day and time of the peak 30-min interval that drove the demand charge. This is the most actionable output — it shows exactly which event (e.g. EV charging during the 4–9pm window) cost the most.
- **Wholesale floor card:** shows what the pure AEMO wholesale cost would have been, with a note that actual Amber pricing would be ~18c/kWh higher due to network/retail stack.
- Recalculates via HTMX partial when date range changes, no full page reload.

#### Plans (`/plans`)

- List all configured tariff plans.
- Create / edit plan form. Rates table within the form uses HTMX to add/remove rate rows dynamically.
- Soft-delete (set `valid_to`) rather than hard delete, to preserve historical bill calculations.

#### Load Events (`/loads`)

- Paginated table of load events: name, start, end, kWh, confidence, source.
- Inline confirm / reject buttons (HTMX partial update).
- Manual add form for events the detector missed.
- Summary panel: EV charging kWh this month, estimated cost under each plan.

### API Routes

```
GET  /api/v1/readings?from=&to=          # paginated solar readings
GET  /api/v1/readings/latest             # most recent reading
GET  /api/v1/summary/daily?from=&to=     # daily aggregates
GET  /api/v1/summary/monthly?month=      # monthly summary (for future Glow-worm integration)
GET  /api/v1/plans                       # list plans
POST /api/v1/plans                       # create plan
PUT  /api/v1/plans/{id}                  # update plan
GET  /api/v1/compare?plan_ids=&from=&to= # bill comparison across plans
GET  /api/v1/loads?from=&to=             # load events
POST /api/v1/loads                       # manual load event
PATCH /api/v1/loads/{id}                 # confirm/reject/update
GET  /api/v1/wholesale/prices?from=&to=  # AEMO price data
```

### Steps

1. Set up Jinja2 template environment with base layout, nav, and HTMX CDN link.
2. Implement dashboard with live refresh partial.
3. Implement history page with Chart.js integration.
4. Implement plan comparison page — this is the core value, prioritise it.
5. Implement plans management forms.
6. Implement load events page.
7. Write API router modules; ensure all pages degrade gracefully if JS is disabled (basic table views).

---

## Phase 5 — Future: Glow-worm Integration

Not in scope for initial build. When ready, the integration surface is deliberately minimal:

**Candela exposes:**
```
GET /api/v1/summary/monthly?month=YYYY-MM
```

Returns:
```json
{
  "month": "2025-06",
  "import_kwh": 312.4,
  "export_kwh": 187.2,
  "solar_kwh": 623.8,
  "estimated_cost_cents": 8420,
  "plan_id": 1,
  "plan_name": "Engie Single Rate"
}
```

**Glow-worm consumes** this endpoint to populate the electricity line item in its true monthly cost / sinking fund view. Glow-worm does not write to Candela's database and does not need to understand tariff structures.

This keeps both apps independently deployable and testable.

---

## Implementation Order for Claude Code

Feed phases sequentially. At the start of each Claude Code session, include:

> "We are building **Candela**, a solar analytics and electricity plan comparison app. The full project plan is in `candela-project-plan.md`. We are now implementing **Phase N — [Phase Name]**. Please implement the steps in order, writing tests first."

**Constraints to include in every session:**
- Python 3.14, `uv` for all dependency management
- `asyncpg` (production) / `aiosqlite` (development) for database access — no ORM. All SQL must be compatible with both backends
- Alembic for migrations, same pattern as the Glow-worm budgeting app. Use `sa.DateTime(timezone=True)` and portable column types throughout
- TDD: write tests before implementation
- HTMX for all UI interactivity, no frontend build pipeline
- Pydantic models for all API request/response shapes
- All configuration via environment variables using `pydantic-settings`
- Type annotations on all functions and methods
- No print statements — use Python `logging` module throughout
