# Group Scholar Review Queue Forecaster Progress

## Iteration 1
- Bootstrapped a Go CLI to summarize review latency and SLA breach risk from CSV exports.
- Added stage-level stats, reviewer coverage counts, JSON output, and sample data.

## Iteration 2
- Added aging buckets (on-time, at-risk, overdue) plus risk tier classification for stages and reviewers.
- Extended console output and JSON reports with the new aging/risk signals.

## Iteration 3
- Added throughput trend comparisons between current and prior windows (overall + stages) with delta and per-week rates.
- Surfaced throughput trend insights in console output and JSON report payloads.

## Iteration 4
- Added queue reviewer forecasts with throughput-based clearance estimates plus assigned vs unassigned counts.
- Extended queue CSV exports with reviewer forecasts and updated sample queue data + README usage.

## Iteration 5
- Added Postgres persistence with schema/table setup, seed run data, and CLI commands to init, save, and list stored runs.
- Updated documentation with database usage and persistence workflow.

## Iteration 6
- Standardized the Postgres schema name and environment variable expectations for production storage.
- Refined database docs and error messaging, and verified production seed data initialization.

## Iteration 7
- Added queue clearance capacity planning with target clear-days, throughput gaps, and capacity status signals.
- Extended queue CSV output, console summary, and seed data to include the new capacity plan.
