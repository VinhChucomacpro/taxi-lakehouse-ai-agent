# Demo Scenario Pack

Last verified: `2026-05-06`

Defense dataset window: `2024-01-01` through `2024-06-30`

Use these scenarios for thesis defense and product-style walkthroughs. They are
designed to show the complete system: schema, deterministic SQL, read-only agent
timeline, curated marts, controlled star-schema access, guardrails, charts, and
CSV export. The Ask AI tab also keeps a session-local history display for demo
continuity; this is not multi-turn agent memory and is not sent to the API.

## Demo Flow

1. Start the stack with `docker compose up -d`.
2. Open Streamlit at `http://localhost:8501`.
3. Check the sidebar status: API is `ok`, DuckDB path is shown, and semantic
   catalog is loaded.
4. Open `Schema` and show that only curated Gold objects are exposed.
5. Run one SQL mart scenario, one star-schema scenario, one Ask AI scenario, one
   chart/export scenario, and one blocked-query scenario.
6. Use Ask AI history to show previous prompts and answers during the same demo
   session without implying context-aware follow-up behavior.
7. Keep prompts and SQL filtered to `2024-H1` when a stable defense result is
   needed.

## Official Scenarios

| ID | UI area | Prompt or action | Expected surface | What to show |
| --- | --- | --- | --- | --- |
| `D01` | Sidebar + Schema | Open app, inspect status and schema | Catalog | API health, DuckDB path, loaded semantic catalog, Gold-only tables |
| `D02` | SQL Test | Default SQL over `gold_daily_kpis` | Aggregate mart | Daily service trip counts filtered to `2024-H1` |
| `D03` | Ask AI | `So sánh số chuyến Yellow Taxi và Green Taxi theo tháng trong nửa đầu năm 2024` | Aggregate mart or validated Gold SQL | Vietnamese question, generated/validated SQL, answer, timeline |
| `D04` | Ask AI | `Top pickup zones by trip count in 2024 H1` | `gold_zone_demand` | Zone demand fast path and optional chart |
| `D05` | Star Schema | Default vendor query | `fact_trips` + `dim_vendor` | Controlled semantic join and top vendors |
| `D06` | Ask AI | `Payment type distribution in 2024 H1` | `fact_trips` + `dim_payment_type` | Star-schema dimension analysis |
| `D07` | Ask AI | `Pickup borough demand in 2024 H1` | `gold_zone_demand` or pickup `dim_zone` join | Geographic demand and charting |
| `D08` | Ask AI | `Dropoff borough demand in 2024 H1` | `fact_trips` + dropoff `dim_zone` role | Difference between pickup and dropoff zone roles |
| `D09` | Ask AI | `trips` | Clarification | Agent asks for metric/grain/time scope instead of executing |
| `D10` | Guardrails | `Silver access` blocked query | Rejection | Bronze/Silver are not exposed to the agent |
| `D11` | Guardrails | `Fact wildcard` blocked query | Rejection | Detailed Gold wildcard access is blocked |
| `D12` | Ask AI | Run two prompts, then clear history | Session-local UI history | Previous Ask AI results display newest-first; clearing history does not call the API |
| `D13` | Any successful result | Enable `Show chart`, then `Export CSV` | Result UX | Human-reviewed chart rendering and reproducible export |
| `D14` | Ask AI | `Average trip distance by service type by month in 2024 H1` | `gold_daily_kpis` | New deterministic service KPI planner coverage |
| `D15` | Ask AI | `Compare pickup and dropoff borough demand in 2024 H1` | `fact_trips` + two `dim_zone` roles | Controlled pickup/dropoff role comparison |

## Scenario Notes

- `D02` demonstrates the aggregate-mart fast path with predictable daily rows.
- `D03` is the primary Vietnamese natural-language scenario.
- `D05`, `D06`, and `D08` demonstrate that fact/dimension access is allowed only
  through semantic catalog tables, cataloged columns, and approved joins.
- `D08` should show the dropoff-zone role explicitly with
  `fact_trips.dropoff_zone_id = dim_zone.zone_id`; it should not reuse the
  pickup-zone mart.
- `D09` demonstrates safe clarification behavior for broad questions.
- `D10` and `D11` demonstrate that the API does not expose raw lakehouse layers
  or uncontrolled detail queries.
- `D12` demonstrates that the demo shows a local history log without turning the
  agent into a multi-turn chat system.
- `D13` should be run after a successful table result so charting and export are
  visible without hiding the underlying rows.
- `D14` and `D15` are post-Phase 27 planner regression scenarios. They are
  useful spot checks after code changes, but the core 10-15 minute defense flow
  can still use `D01` through `D13`.

## Expected Evidence

For successful scenarios, the UI should show:

- final answer
- agent timeline
- SQL expander
- row/column/execution metrics
- result table
- optional chart after enabling `Show chart`
- `Export CSV`

For clarification scenarios, the UI should show:

- agent timeline
- clarification prompt
- no table/chart output

For blocked scenarios, the UI should show:

- error message with HTTP `400`
- no DuckDB result rows
- no unsafe query execution

## Caveats

- The Ask AI tab may use deterministic planning or LLM planning depending on the
  prompt. Guardrails remain the enforcement point before execution.
- OpenAI answer synthesis is optional. The default demo can use deterministic
  answers grounded in returned rows.
- The warehouse contains data outside `2024-H1`; official scenarios should keep
  the defense-window time filter when stable results matter.
