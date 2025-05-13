# Group Scholar Review Queue Forecaster

Local-first CLI that estimates review latency, SLA breach risk, reviewer coverage, and queue clearance projections from review event CSVs.

## Features
- Stage-level latency stats (average, median, p90, max)
- SLA breach counts and rates
- Distinct reviewer coverage per stage
- Aging buckets (on time, at risk, overdue) with risk tiers
- Reviewer throughput snapshots with last-reviewed timestamp
- Throughput trend comparison versus prior window (overall + top stages)
- Queue forecast with due-soon/overdue counts, clearance estimates, and assigned vs unassigned split
- Reviewer-level queue forecast with throughput-based clear days
- JSON output for downstream reporting

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
- Flag stages with rising latency versus prior weeks.
- Export CSV summaries for weekly ops reviews.
