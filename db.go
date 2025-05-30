package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type DBConfig struct {
	DSN    string
	Schema string
}

type RunInsert struct {
	GeneratedAt    time.Time
	InputPath      string
	QueuePath      string
	SLADays        int
	ThroughputDays int
	TotalEvents    int
	ReportJSON     []byte
	QueueJSON      []byte
}

type RunSummary struct {
	ID            int64
	CreatedAt     time.Time
	GeneratedAt   time.Time
	TotalEvents   int
	SLADays       int
	Throughput    int
	QueuePending  sql.NullInt64
	QueueAssigned sql.NullInt64
	QueueOverdue  sql.NullInt64
}

func resolveDBConfig(dsnFlag string, schema string) (DBConfig, error) {
	dsn := strings.TrimSpace(dsnFlag)
	if dsn == "" {
		dsn = strings.TrimSpace(os.Getenv("GS_REVIEW_QUEUE_FORECASTER_DB_URL"))
	}
	if dsn == "" {
		dsn = strings.TrimSpace(os.Getenv("GS_REVIEW_QUEUE_DB_URL"))
	}
	if dsn == "" {
		dsn = strings.TrimSpace(os.Getenv("DATABASE_URL"))
	}
	if dsn == "" {
		return DBConfig{}, errors.New("database DSN missing: set --db-url, GS_REVIEW_QUEUE_FORECASTER_DB_URL, GS_REVIEW_QUEUE_DB_URL, or DATABASE_URL")
	}
	if strings.TrimSpace(schema) == "" {
		schema = "gs_review_queue_forecaster"
	}
	return DBConfig{DSN: dsn, Schema: schema}, nil
}

func openDB(cfg DBConfig) (*sql.DB, error) {
	db, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func ensureSchema(ctx context.Context, db *sql.DB, schema string) error {
	if schema == "" {
		schema = "gs_review_queue_forecaster"
	}
	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", pqQuoteIdentifier(schema)))
	return err
}

func ensureRunsTable(ctx context.Context, db *sql.DB, schema string) error {
	query := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.review_runs (
	id BIGSERIAL PRIMARY KEY,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	generated_at TIMESTAMPTZ NOT NULL,
	input_path TEXT,
	queue_path TEXT,
	sla_days INT NOT NULL,
	throughput_days INT NOT NULL,
	total_events INT NOT NULL,
	report JSONB NOT NULL,
	queue_summary JSONB
);
CREATE INDEX IF NOT EXISTS review_runs_created_at_idx ON %s.review_runs (created_at DESC);
`, pqQuoteIdentifier(schema), pqQuoteIdentifier(schema))

	_, err := db.ExecContext(ctx, query)
	return err
}

func seedRuns(ctx context.Context, db *sql.DB, schema string) (bool, error) {
	var count int
	row := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s.review_runs", pqQuoteIdentifier(schema)))
	if err := row.Scan(&count); err != nil {
		return false, err
	}
	if count > 0 {
		return false, nil
	}

	seedNow := time.Now().AddDate(0, 0, -1)
	seedReport := Report{
		GeneratedAt: seedNow.Format(time.RFC3339),
		TotalEvents: 48,
		Overall: StageStats{
			Stage:             "overall",
			Count:             48,
			AverageDays:       6.4,
			MedianDays:        6.0,
			P90Days:           11.2,
			MaxDays:           14.5,
			SLABreachCount:    8,
			SLABreachRate:     16.7,
			DistinctReviewers: 9,
			AgingBuckets:      AgingBuckets{OnTime: 36, AtRisk: 9, Overdue: 3},
			RiskTier:          "medium",
		},
		SLADays:         10,
		Throughput:      ThroughputSummary{AsOf: seedNow.Format(time.RFC3339), WindowDays: 28, EventsInWindow: 42, ThroughputPerWeek: 10.5},
		ThroughputTrend: ThroughputTrendSummary{CurrentWindowStart: seedNow.AddDate(0, 0, -28).Format(time.RFC3339), CurrentWindowEnd: seedNow.Format(time.RFC3339), PriorWindowStart: seedNow.AddDate(0, 0, -56).Format(time.RFC3339), PriorWindowEnd: seedNow.AddDate(0, 0, -28).Format(time.RFC3339), WindowDays: 28, Trends: []ThroughputTrend{buildTrend("overall", 42, 38, 28)}},
		Queue: &QueueReport{
			AsOf:            seedNow.Format(time.RFC3339),
			TotalPending:    18,
			AssignedCount:   12,
			UnassignedCount: 6,
			OverdueCount:    4,
			DueSoonCount:    5,
			OnTrackCount:    9,
			AvgAgeDays:      5.3,
			ThroughputDays:  28,
			DueSoonRatio:    0.8,
		},
	}
	seedQueueSummary := map[string]int{"total_pending": 18, "assigned_count": 12, "unassigned_count": 6, "overdue_count": 4}

	reportJSON, err := json.Marshal(seedReport)
	if err != nil {
		return false, err
	}
	queueJSON, err := json.Marshal(seedQueueSummary)
	if err != nil {
		return false, err
	}

	insert := fmt.Sprintf(`
INSERT INTO %s.review_runs (generated_at, input_path, queue_path, sla_days, throughput_days, total_events, report, queue_summary)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
`, pqQuoteIdentifier(schema))

	_, err = db.ExecContext(ctx, insert, seedNow, "seed:sample-events.csv", "seed:sample-queue.csv", 10, 28, 48, reportJSON, queueJSON)
	if err != nil {
		return false, err
	}
	return true, nil
}

func insertRun(ctx context.Context, db *sql.DB, schema string, run RunInsert) error {
	query := fmt.Sprintf(`
INSERT INTO %s.review_runs (generated_at, input_path, queue_path, sla_days, throughput_days, total_events, report, queue_summary)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
`, pqQuoteIdentifier(schema))
	_, err := db.ExecContext(ctx, query, run.GeneratedAt, run.InputPath, run.QueuePath, run.SLADays, run.ThroughputDays, run.TotalEvents, run.ReportJSON, nullableJSON(run.QueueJSON))
	return err
}

func listRuns(ctx context.Context, db *sql.DB, schema string, limit int) ([]RunSummary, error) {
	if limit <= 0 {
		limit = 5
	}
	query := fmt.Sprintf(`
SELECT id, created_at, generated_at, total_events, sla_days, throughput_days,
	(queue_summary->>'total_pending')::INT AS total_pending,
	(queue_summary->>'assigned_count')::INT AS assigned_count,
	(queue_summary->>'overdue_count')::INT AS overdue_count
FROM %s.review_runs
ORDER BY created_at DESC
LIMIT $1
`, pqQuoteIdentifier(schema))

	rows, err := db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var summaries []RunSummary
	for rows.Next() {
		var summary RunSummary
		if err := rows.Scan(&summary.ID, &summary.CreatedAt, &summary.GeneratedAt, &summary.TotalEvents, &summary.SLADays, &summary.Throughput, &summary.QueuePending, &summary.QueueAssigned, &summary.QueueOverdue); err != nil {
			return nil, err
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return summaries, nil
}

func nullableJSON(payload []byte) any {
	if len(payload) == 0 {
		return nil
	}
	return payload
}

func pqQuoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func parseLimit(input string) int {
	input = strings.TrimSpace(input)
	if input == "" {
		return 0
	}
	value, err := strconv.Atoi(input)
	if err != nil {
		return 0
	}
	return value
}
