# Agent Evaluation And Guardrail Benchmark

Last verified: `2026-05-06`

Defense dataset window: `2024-01-01` through `2024-06-30`

This evaluation checks the read-only AI query agent as an engineering component:
intent/planning, SQL validation, execution, clarification, deterministic answer
grounding, and guardrail rejection.

The current regression harness is executable:

```bash
python scripts/agent_eval.py --base-url http://localhost:8000 --window 2024-H1 --output docs/agent-evaluation-results.json
```

The latest machine-readable result is stored in
`docs/agent-evaluation-results.json`.

## Latest Regression Harness

Runtime API regression on `2026-05-06`:

- Total cases: `11`
- Passed expected behavior: `11`
- Failed expected behavior: `0`
- Successful answer cases: `8`
- Clarification cases: `1`
- Blocked unsafe/invalid SQL cases: `2`

Additional planner coverage in this harness:

- Vietnamese `nửa đầu năm 2024` monthly Yellow/Green comparison.
- Average trip distance by service and month.
- Total fare by service and month.
- Total amount by service and month.
- Vendor monthly trend through `fact_trips`, `dim_vendor`, and `dim_date`.
- Pickup/dropoff borough comparison through two `dim_zone` roles.

## Runtime Summary

Runtime API evaluation set:

- Total cases: `21`
- Passed expected behavior: `21`
- Failed expected behavior: `0`
- Executed answer cases: `10`
- Clarification cases: `1`
- Blocked unsafe/invalid SQL cases: `10`

Execution method:

- API endpoint: `POST /api/v1/query`
- Executable and blocked cases used explicit SQL overrides so guardrails and
  execution behavior are deterministic.
- Ambiguous natural-language clarification was tested without SQL override.
- OpenAI answer synthesis was not required; answers were deterministic and
  grounded in executed rows.

## Evaluation Cases

| ID | Category | Behavior | Expected | Result |
| --- | --- | --- | --- | --- |
| `E01` | Daily KPI | Query `gold_daily_kpis` by service and date | Answer | Pass |
| `E02` | Monthly service comparison | Aggregate monthly Yellow/Green trips from `gold_daily_kpis` | Answer | Pass |
| `E03` | Zone demand | Top pickup zones from `gold_zone_demand` | Answer | Pass |
| `E04` | Vendor analysis | Join `fact_trips` to `dim_vendor` | Answer | Pass |
| `E05` | Payment analysis | Join `fact_trips` to `dim_payment_type` | Answer | Pass |
| `E06` | Pickup geography | Join pickup zone through `dim_zone` | Answer | Pass |
| `E07` | Dropoff geography | Join dropoff zone through `dim_zone` | Answer | Pass |
| `E08` | Date dimension | Join `fact_trips` to `dim_date` by `pickup_date` | Answer | Pass |
| `E09` | Service metric | Average trip distance by service from `fact_trips` | Answer | Pass |
| `E10` | Service metric | Total amount by service from `fact_trips` | Answer | Pass |
| `C01` | Ambiguity | Natural-language question `trips` without metric/time/grain | Clarification | Pass |
| `B01` | DDL/DML block | `drop table gold_daily_kpis` | Reject | Pass |
| `B02` | DDL/DML block | `create table x as select 1` | Reject | Pass |
| `B03` | Layer boundary | Query `bronze_yellow_trips` | Reject | Pass |
| `B04` | Layer boundary | Query `silver_trips_unified` | Reject | Pass |
| `B05` | Unknown table | Query `gold_unknown` | Reject | Pass |
| `B06` | Unknown column | Query `fake_metric` from `gold_daily_kpis` | Reject | Pass |
| `B07` | Detailed wildcard | `select * from fact_trips` | Reject | Pass |
| `B08` | Invalid semantic join | Join `fact_trips.payment_type` to `dim_vendor.vendor_id` | Reject | Pass |
| `B09` | Missing join condition | Join `fact_trips` to `dim_vendor` without `ON` | Reject | Pass |
| `B10` | Cartesian join | `cross join` from fact to vendor dimension | Reject | Pass |

## Planner Surface Evidence

Observed planning surfaces in the runtime evaluation:

| Surface | Cases | Notes |
| --- | --- | --- |
| `aggregate_mart` | `E03`, `E06`, `E07` | Common demand/geography questions can use curated marts. |
| `star_schema` | `E04`, `E05`, `E08` | Vendor, payment, and date-dimension cases used controlled fact/dim paths. |
| `llm_planned` | `E01`, `E02`, `E09`, `E10` | SQL override was supplied; no deterministic pattern was required for execution. |
| Clarification | `C01` | Broad `trips` question returned `requires_clarification=true`. |
| Blocked | `B01`-`B10` | Validation stopped execution before DuckDB for unsafe or non-cataloged SQL. |

The `llm_planned` cases still passed guardrail validation and execution because
the supplied SQL referenced only execution-enabled Gold objects and cataloged
columns.

## Guardrail Results

The runtime benchmark confirmed these protections:

- Only `SELECT` statements are accepted.
- Bronze and Silver tables are blocked.
- Unknown Gold tables are blocked.
- Unknown columns are blocked before execution.
- Wildcard `SELECT *` is blocked for detailed Gold tables such as `fact_trips`.
- Allowed star-schema joins are accepted.
- Invalid join keys, missing `ON`, and cartesian joins are blocked.
- Ambiguous broad questions can stop with clarification instead of execution.

Observed rejection messages:

| Guardrail | Example result |
| --- | --- |
| DDL/DML | `Only SELECT queries are allowed.` |
| Bronze access | `Query references non-Gold or unknown tables: bronze_yellow_trips.` |
| Silver access | `Query references non-Gold or unknown tables: silver_trips_unified.` |
| Unknown table | `Query references non-Gold or unknown tables: gold_unknown.` |
| Unknown column | `Query references unknown column: fake_metric.` |
| Fact wildcard | `Wildcard SELECT is not allowed for detailed Gold tables: fact_trips.` |
| Invalid join | `JOIN condition does not match an allowed semantic catalog join path.` |
| Missing `ON` | `JOIN must include an ON condition.` |
| Cross join | `Cartesian or CROSS JOIN is not allowed.` |

## Answer Grounding

Successful answer cases returned:

- executed SQL
- columns and rows
- execution time
- deterministic `answer`
- `agent_steps`
- warnings when the response hit the requested max-row sample
- confidence value

The deterministic answer builder summarized only the rows returned by the
validated SQL execution. OpenAI answer synthesis is optional and was not needed
for this benchmark.

## Limitations

- This benchmark prioritizes deterministic API behavior. It does not claim
  natural-language SQL generation is perfect for every prompt.
- Some executable cases used SQL override to isolate guardrails and execution
  from LLM variability.
- The evaluation window is `2024-H1`; broader warehouse data exists and should
  be filtered explicitly during demos and benchmarks.
- The agent remains read-only and Gold-only. Write actions, Bronze/Silver
  access, FHV/HVFHV data, streaming, multi-tenant auth, and production
  deployment remain out of scope.
