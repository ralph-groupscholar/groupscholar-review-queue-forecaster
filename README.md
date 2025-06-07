# Group Scholar Review Queue Forecaster

Local-first CLI that estimates review latency, SLA breach risk, reviewer coverage, queue clearance projections, and persists run snapshots to Postgres.

## Features
- Stage-level latency stats (average, median, p90, max)
- SLA breach counts and rates
- Distinct reviewer coverage per stage
- Aging buckets (on time, at risk, overdue) with risk tiers
- Reviewer throughput snapshots with last-reviewed timestamp
- Throughput trend comparison versus prior window (overall + top stages)
- Latency trend comparison between windows (overall + top stages)
- Insight deck highlighting SLA, throughput, latency, and queue risks
- Queue forecast with due-soon/overdue counts, clearance estimates, and assigned vs unassigned split
- Queue clearance capacity plan with target clear-days and throughput gaps
- Reviewer-level queue forecast with throughput-based clear days
- Insight deck CSV export for weekly ops reviews
- Queue priority CSV export for top SLA-risk items
- JSON output for downstream reporting
- Postgres persistence with seed data for live dashboards

## Quickstart
```bash
go run . --input data/sample-events.csv
```

```bash
go run . --input data/sample-events.csv --sla-days 7 --json
```

```bash
go run . --input data/sample-events.csv --reviewer-top 3
```

```bash
go run . --input data/sample-events.csv --throughput-days 21
```

```bash
go run . --input data/sample-events.csv --queue data/sample-queue.csv
```

```bash
go run . --input data/sample-events.csv --queue data/sample-queue.csv --csv-out exports/review-queue
```

```bash
go run . --input data/sample-events.csv --queue data/sample-queue.csv --csv-out exports/review-queue --brief-out exports/review-brief.md
```

```bash
go run . --input data/sample-events.csv --queue data/sample-queue.csv --target-clear-days 10
```

```bash
go run . --input data/sample-events.csv --queue data/sample-queue.csv --brief-out exports/review-queue-brief.md
```

## Postgres Persistence
Set `GS_REVIEW_QUEUE_DB_URL` (production only) or pass `--db-url` to store run snapshots. The CLI creates a schema + table and seeds a sample run if the table is empty.

```bash
go run . --db-init
```

```bash
go run . --input data/sample-events.csv --queue data/sample-queue.csv --store-db
```

```bash
go run . --db-list 10
```

## CSV Format
Required columns:
- application_id
- stage
- submitted_at
- reviewed_at
- reviewer_id

Accepted date formats: RFC3339, `YYYY-MM-DD`, or `YYYY-MM-DD HH:MM:SS`.

Queue CSV columns:
- application_id
- stage
- submitted_at
- reviewer_id (optional)

## Example Output
```
Review Queue Forecaster
Generated: 2026-02-07T17:20:00Z
SLA Days: 10
Total Events: 8

Overall
- overall
  Count: 8 | Avg: 7.62 days | Median: 7.00 days | P90: 12.00 days | Max: 12.00 days
  SLA Breach: 3 (37.5%) | Distinct Reviewers: 5
  Aging: On Time 5 (62.5%) | At Risk 2 (25.0%) | Overdue 1 (12.5%) | Risk Tier: medium
```

## Next Ideas
- Add a small web UI that charts saved runs from Postgres.
- Flag stages with rising latency versus prior weeks.
- Export CSV summaries for weekly ops reviews.
