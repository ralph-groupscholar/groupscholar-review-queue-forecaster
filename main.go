package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ReviewEvent struct {
	ApplicationID string
	Stage         string
	SubmittedAt   time.Time
	ReviewedAt    time.Time
	ReviewerID    string
}

type QueueItem struct {
	ApplicationID string
	Stage         string
	SubmittedAt   time.Time
	ReviewerID    string
}

type StageStats struct {
	Stage             string       `json:"stage"`
	Count             int          `json:"count"`
	AverageDays       float64      `json:"average_days"`
	MedianDays        float64      `json:"median_days"`
	P90Days           float64      `json:"p90_days"`
	MaxDays           float64      `json:"max_days"`
	SLABreachCount    int          `json:"sla_breach_count"`
	SLABreachRate     float64      `json:"sla_breach_rate"`
	DistinctReviewers int          `json:"distinct_reviewers"`
	AgingBuckets      AgingBuckets `json:"aging_buckets"`
	RiskTier          string       `json:"risk_tier"`
}

type AgingBuckets struct {
	OnTime  int `json:"on_time"`
	AtRisk  int `json:"at_risk"`
	Overdue int `json:"overdue"`
}

type ReviewerStats struct {
	ReviewerID        string       `json:"reviewer_id"`
	Count             int          `json:"count"`
	AverageDays       float64      `json:"average_days"`
	MedianDays        float64      `json:"median_days"`
	P90Days           float64      `json:"p90_days"`
	MaxDays           float64      `json:"max_days"`
	SLABreachCount    int          `json:"sla_breach_count"`
	SLABreachRate     float64      `json:"sla_breach_rate"`
	LastReviewedAt    string       `json:"last_reviewed_at"`
	ThroughputPerWeek float64      `json:"throughput_per_week"`
	WindowCount       int          `json:"window_count"`
	AgingBuckets      AgingBuckets `json:"aging_buckets"`
	RiskTier          string       `json:"risk_tier"`
}

type ThroughputSummary struct {
	AsOf              string  `json:"as_of"`
	WindowDays        int     `json:"window_days"`
	EventsInWindow    int     `json:"events_in_window"`
	ThroughputPerWeek float64 `json:"throughput_per_week"`
}

type ThroughputTrend struct {
	Label          string  `json:"label"`
	CurrentCount   int     `json:"current_count"`
	PriorCount     int     `json:"prior_count"`
	Delta          int     `json:"delta"`
	DeltaPercent   float64 `json:"delta_percent"`
	CurrentPerWeek float64 `json:"current_per_week"`
	PriorPerWeek   float64 `json:"prior_per_week"`
	Trend          string  `json:"trend"`
}

type ThroughputTrendSummary struct {
	CurrentWindowStart string            `json:"current_window_start"`
	CurrentWindowEnd   string            `json:"current_window_end"`
	PriorWindowStart   string            `json:"prior_window_start"`
	PriorWindowEnd     string            `json:"prior_window_end"`
	WindowDays         int               `json:"window_days"`
	Trends             []ThroughputTrend `json:"trends"`
}

type LatencyTrend struct {
	Label             string  `json:"label"`
	CurrentCount      int     `json:"current_count"`
	PriorCount        int     `json:"prior_count"`
	CurrentAvgDays    float64 `json:"current_avg_days"`
	PriorAvgDays      float64 `json:"prior_avg_days"`
	AvgDeltaDays      float64 `json:"avg_delta_days"`
	AvgDeltaPercent   float64 `json:"avg_delta_percent"`
	CurrentMedianDays float64 `json:"current_median_days"`
	PriorMedianDays   float64 `json:"prior_median_days"`
	MedianDeltaDays   float64 `json:"median_delta_days"`
	MedianDeltaPct    float64 `json:"median_delta_pct"`
	Trend             string  `json:"trend"`
}

type LatencyTrendSummary struct {
	CurrentWindowStart string          `json:"current_window_start"`
	CurrentWindowEnd   string          `json:"current_window_end"`
	PriorWindowStart   string          `json:"prior_window_start"`
	PriorWindowEnd     string          `json:"prior_window_end"`
	WindowDays         int             `json:"window_days"`
	Trends             []LatencyTrend  `json:"trends"`
}

type QueueStageForecast struct {
	Stage              string  `json:"stage"`
	PendingCount       int     `json:"pending_count"`
	AvgAgeDays         float64 `json:"avg_age_days"`
	OverdueCount       int     `json:"overdue_count"`
	DueSoonCount       int     `json:"due_soon_count"`
	OnTrackCount       int     `json:"on_track_count"`
	DailyThroughput    float64 `json:"daily_throughput"`
	EstimatedClearDays float64 `json:"estimated_clear_days"`
	ClearanceStatus    string  `json:"clearance_status"`
}

type QueueReviewerForecast struct {
	ReviewerID         string  `json:"reviewer_id"`
	PendingCount       int     `json:"pending_count"`
	AvgAgeDays         float64 `json:"avg_age_days"`
	OverdueCount       int     `json:"overdue_count"`
	DueSoonCount       int     `json:"due_soon_count"`
	OnTrackCount       int     `json:"on_track_count"`
	ThroughputPerWeek  float64 `json:"throughput_per_week"`
	EstimatedClearDays float64 `json:"estimated_clear_days"`
	ClearanceStatus    string  `json:"clearance_status"`
}

type QueueReport struct {
	AsOf            string                  `json:"as_of"`
	TotalPending    int                     `json:"total_pending"`
	AssignedCount   int                     `json:"assigned_count"`
	UnassignedCount int                     `json:"unassigned_count"`
	OverdueCount    int                     `json:"overdue_count"`
	DueSoonCount    int                     `json:"due_soon_count"`
	OnTrackCount    int                     `json:"on_track_count"`
	AvgAgeDays      float64                 `json:"avg_age_days"`
	Stages          []QueueStageForecast    `json:"stages"`
	Reviewers       []QueueReviewerForecast `json:"reviewers"`
	ThroughputDays  int                     `json:"throughput_days"`
	DueSoonRatio    float64                 `json:"due_soon_ratio"`
}

type Report struct {
	GeneratedAt     string                 `json:"generated_at"`
	TotalEvents     int                    `json:"total_events"`
	Overall         StageStats             `json:"overall"`
	Stages          []StageStats           `json:"stages"`
	Reviewers       []ReviewerStats        `json:"reviewers"`
	SLADays         int                    `json:"sla_days"`
	Throughput      ThroughputSummary      `json:"throughput"`
	ThroughputTrend ThroughputTrendSummary `json:"throughput_trend"`
	LatencyTrend    LatencyTrendSummary    `json:"latency_trend"`
	Queue           *QueueReport           `json:"queue,omitempty"`
}

func main() {
	inputPath := flag.String("input", "data/sample-events.csv", "Path to review events CSV")
	queuePath := flag.String("queue", "", "Path to pending queue CSV (optional)")
	slaDays := flag.Int("sla-days", 10, "SLA threshold in days")
	throughputDays := flag.Int("throughput-days", 28, "Window in days for throughput metrics")
	asOfInput := flag.String("as-of", "", "As-of date for throughput window (defaults to latest reviewed_at)")
	dueSoonRatio := flag.Float64("due-soon-ratio", 0.8, "Fraction of SLA days considered due soon")
	jsonOutput := flag.Bool("json", false, "Emit JSON output")
	csvOut := flag.String("csv-out", "", "Write CSV summaries using this path prefix or directory")
	reviewerTop := flag.Int("reviewer-top", 5, "Top reviewers to show by throughput")
	flag.Parse()

	events, err := loadEvents(*inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load events: %v\n", err)
		os.Exit(1)
	}

	var queueItems []QueueItem
	if strings.TrimSpace(*queuePath) != "" {
		queueItems, err = loadQueue(*queuePath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to load queue: %v\n", err)
			os.Exit(1)
		}
	}

	report, err := buildReport(events, queueItems, *slaDays, *throughputDays, *asOfInput, *dueSoonRatio)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build report: %v\n", err)
		os.Exit(1)
	}
	if strings.TrimSpace(*csvOut) != "" {
		if err := writeCSVReports(report, *csvOut); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write csv output: %v\n", err)
			os.Exit(1)
		}
	}
	if *jsonOutput {
		payload, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to encode json: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(string(payload))
		return
	}

	printReport(report, *reviewerTop)
}

func loadEvents(path string) ([]ReviewEvent, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 {
		return nil, errors.New("CSV must include header and at least one row")
	}

	header := normalizeHeader(records[0])
	idx := map[string]int{}
	for i, name := range header {
		idx[name] = i
	}

	required := []string{"application_id", "stage", "submitted_at", "reviewed_at", "reviewer_id"}
	for _, key := range required {
		if _, ok := idx[key]; !ok {
			return nil, fmt.Errorf("missing required column: %s", key)
		}
	}

	var events []ReviewEvent
	for rowIndex, row := range records[1:] {
		if len(row) == 0 {
			continue
		}
		event, err := parseRow(row, idx)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", rowIndex+2, err)
		}
		events = append(events, event)
	}
	return events, nil
}

func loadQueue(path string) ([]QueueItem, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 {
		return nil, errors.New("queue CSV must include header and at least one row")
	}

	header := normalizeHeader(records[0])
	idx := map[string]int{}
	for i, name := range header {
		idx[name] = i
	}

	required := []string{"application_id", "stage", "submitted_at"}
	for _, key := range required {
		if _, ok := idx[key]; !ok {
			return nil, fmt.Errorf("missing required column: %s", key)
		}
	}

	var items []QueueItem
	for rowIndex, row := range records[1:] {
		if len(row) == 0 {
			continue
		}
		item, err := parseQueueRow(row, idx)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", rowIndex+2, err)
		}
		items = append(items, item)
	}
	return items, nil
}

func normalizeHeader(header []string) []string {
	out := make([]string, len(header))
	for i, name := range header {
		out[i] = strings.ToLower(strings.TrimSpace(name))
	}
	return out
}

func parseRow(row []string, idx map[string]int) (ReviewEvent, error) {
	get := func(key string) string {
		pos := idx[key]
		if pos >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[pos])
	}

	submittedAt, err := parseDate(get("submitted_at"))
	if err != nil {
		return ReviewEvent{}, fmt.Errorf("invalid submitted_at: %w", err)
	}
	reviewedAt, err := parseDate(get("reviewed_at"))
	if err != nil {
		return ReviewEvent{}, fmt.Errorf("invalid reviewed_at: %w", err)
	}
	if reviewedAt.Before(submittedAt) {
		return ReviewEvent{}, errors.New("reviewed_at is before submitted_at")
	}

	return ReviewEvent{
		ApplicationID: get("application_id"),
		Stage:         get("stage"),
		SubmittedAt:   submittedAt,
		ReviewedAt:    reviewedAt,
		ReviewerID:    get("reviewer_id"),
	}, nil
}

func parseQueueRow(row []string, idx map[string]int) (QueueItem, error) {
	get := func(key string) string {
		pos, ok := idx[key]
		if !ok || pos >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[pos])
	}

	submittedAt, err := parseDate(get("submitted_at"))
	if err != nil {
		return QueueItem{}, fmt.Errorf("invalid submitted_at: %w", err)
	}

	return QueueItem{
		ApplicationID: get("application_id"),
		Stage:         get("stage"),
		SubmittedAt:   submittedAt,
		ReviewerID:    get("reviewer_id"),
	}, nil
}

func parseDate(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, errors.New("empty date")
	}

	layouts := []string{time.RFC3339, "2006-01-02", "2006-01-02 15:04:05"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, value); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported format: %s", value)
}

func buildReport(events []ReviewEvent, queueItems []QueueItem, slaDays int, throughputDays int, asOfInput string, dueSoonRatio float64) (Report, error) {
	stageBuckets := map[string][]ReviewEvent{}
	for _, event := range events {
		stageBuckets[event.Stage] = append(stageBuckets[event.Stage], event)
	}

	stages := make([]StageStats, 0, len(stageBuckets))
	for stage, bucket := range stageBuckets {
		stages = append(stages, buildStageStats(stage, bucket, slaDays))
	}

	sort.Slice(stages, func(i, j int) bool {
		return stages[i].AverageDays > stages[j].AverageDays
	})

	overall := buildStageStats("overall", events, slaDays)
	asOf, err := resolveAsOf(events, asOfInput)
	if err != nil {
		return Report{}, err
	}
	throughput, reviewers, err := buildThroughput(events, slaDays, throughputDays, asOf)
	if err != nil {
		return Report{}, err
	}
	trend := buildThroughputTrends(events, asOf, throughputDays)
	latencyTrend := buildLatencyTrends(events, asOf, throughputDays)
	queueReport := buildQueueReport(queueItems, events, slaDays, throughputDays, asOf, dueSoonRatio)

	return Report{
		GeneratedAt:     time.Now().Format(time.RFC3339),
		TotalEvents:     len(events),
		Overall:         overall,
		Stages:          stages,
		Reviewers:       reviewers,
		SLADays:         slaDays,
		Throughput:      throughput,
		ThroughputTrend: trend,
		LatencyTrend:    latencyTrend,
		Queue:           queueReport,
	}, nil
}

func buildStageStats(stage string, events []ReviewEvent, slaDays int) StageStats {
	if len(events) == 0 {
		return StageStats{Stage: stage}
	}

	durations := make([]float64, 0, len(events))
	reviewerSet := map[string]struct{}{}
	breachCount := 0
	buckets := AgingBuckets{}

	for _, event := range events {
		days := event.ReviewedAt.Sub(event.SubmittedAt).Hours() / 24
		durations = append(durations, days)
		if days >= float64(slaDays) {
			breachCount++
		}
		switch {
		case days <= float64(slaDays):
			buckets.OnTime++
		case days <= float64(slaDays*2):
			buckets.AtRisk++
		default:
			buckets.Overdue++
		}
		if event.ReviewerID != "" {
			reviewerSet[event.ReviewerID] = struct{}{}
		}
	}

	sort.Float64s(durations)

	avg := average(durations)
	median := percentile(durations, 50)
	p90 := percentile(durations, 90)
	max := durations[len(durations)-1]
	breachRate := float64(breachCount) / float64(len(durations))
	riskTier := classifyRisk(avg, breachRate, slaDays)

	return StageStats{
		Stage:             stage,
		Count:             len(durations),
		AverageDays:       round(avg, 2),
		MedianDays:        round(median, 2),
		P90Days:           round(p90, 2),
		MaxDays:           round(max, 2),
		SLABreachCount:    breachCount,
		SLABreachRate:     round(breachRate*100, 1),
		DistinctReviewers: len(reviewerSet),
		AgingBuckets:      buckets,
		RiskTier:          riskTier,
	}
}

func buildThroughput(events []ReviewEvent, slaDays int, throughputDays int, asOf time.Time) (ThroughputSummary, []ReviewerStats, error) {
	if throughputDays <= 0 {
		return ThroughputSummary{}, nil, errors.New("throughput-days must be positive")
	}
	windowStart := asOf.AddDate(0, 0, -throughputDays)

	totalInWindow := 0
	for _, event := range events {
		if !event.ReviewedAt.Before(windowStart) && !event.ReviewedAt.After(asOf) {
			totalInWindow++
		}
	}

	reviewers := buildReviewerStats(events, slaDays, windowStart, asOf, throughputDays)

	throughput := ThroughputSummary{
		AsOf:              asOf.Format(time.RFC3339),
		WindowDays:        throughputDays,
		EventsInWindow:    totalInWindow,
		ThroughputPerWeek: round(float64(totalInWindow)/(float64(throughputDays)/7.0), 2),
	}
	return throughput, reviewers, nil
}

func buildThroughputTrends(events []ReviewEvent, asOf time.Time, throughputDays int) ThroughputTrendSummary {
	if throughputDays <= 0 {
		return ThroughputTrendSummary{}
	}
	currentStart := asOf.AddDate(0, 0, -throughputDays)
	currentEnd := asOf
	priorEnd := currentStart
	priorStart := priorEnd.AddDate(0, 0, -throughputDays)

	stageCurrent := map[string]int{}
	stagePrior := map[string]int{}
	currentTotal := 0
	priorTotal := 0

	for _, event := range events {
		switch {
		case inWindow(event.ReviewedAt, currentStart, currentEnd, true):
			stageCurrent[event.Stage]++
			currentTotal++
		case inWindow(event.ReviewedAt, priorStart, priorEnd, false):
			stagePrior[event.Stage]++
			priorTotal++
		}
	}

	trends := []ThroughputTrend{buildTrend("overall", currentTotal, priorTotal, throughputDays)}

	stages := map[string]struct{}{}
	for stage := range stageCurrent {
		stages[stage] = struct{}{}
	}
	for stage := range stagePrior {
		stages[stage] = struct{}{}
	}

	stageTrends := make([]ThroughputTrend, 0, len(stages))
	for stage := range stages {
		stageTrends = append(stageTrends, buildTrend(stage, stageCurrent[stage], stagePrior[stage], throughputDays))
	}

	sort.Slice(stageTrends, func(i, j int) bool {
		if stageTrends[i].CurrentCount == stageTrends[j].CurrentCount {
			return stageTrends[i].Delta > stageTrends[j].Delta
		}
		return stageTrends[i].CurrentCount > stageTrends[j].CurrentCount
	})

	trends = append(trends, stageTrends...)

	return ThroughputTrendSummary{
		CurrentWindowStart: currentStart.Format(time.RFC3339),
		CurrentWindowEnd:   currentEnd.Format(time.RFC3339),
		PriorWindowStart:   priorStart.Format(time.RFC3339),
		PriorWindowEnd:     priorEnd.Format(time.RFC3339),
		WindowDays:         throughputDays,
		Trends:             trends,
	}
}

func buildLatencyTrends(events []ReviewEvent, asOf time.Time, windowDays int) LatencyTrendSummary {
	if windowDays <= 0 {
		return LatencyTrendSummary{}
	}
	currentStart := asOf.AddDate(0, 0, -windowDays)
	currentEnd := asOf
	priorEnd := currentStart
	priorStart := priorEnd.AddDate(0, 0, -windowDays)

	type durationsByStage map[string][]float64
	currentDurations := durationsByStage{}
	priorDurations := durationsByStage{}

	for _, event := range events {
		days := event.ReviewedAt.Sub(event.SubmittedAt).Hours() / 24
		switch {
		case inWindow(event.ReviewedAt, currentStart, currentEnd, true):
			currentDurations[event.Stage] = append(currentDurations[event.Stage], days)
		case inWindow(event.ReviewedAt, priorStart, priorEnd, false):
			priorDurations[event.Stage] = append(priorDurations[event.Stage], days)
		}
	}

	labels := map[string]struct{}{}
	for stage := range currentDurations {
		labels[stage] = struct{}{}
	}
	for stage := range priorDurations {
		labels[stage] = struct{}{}
	}
	labels["overall"] = struct{}{}

	trends := make([]LatencyTrend, 0, len(labels))
	for stage := range labels {
		current := currentDurations[stage]
		prior := priorDurations[stage]
		if stage == "overall" {
			current = flattenDurations(currentDurations)
			prior = flattenDurations(priorDurations)
		}
		trends = append(trends, buildLatencyTrend(stage, current, prior))
	}

	sort.Slice(trends, func(i, j int) bool {
		if trends[i].Label == "overall" {
			return true
		}
		if trends[j].Label == "overall" {
			return false
		}
		if trends[i].CurrentAvgDays == trends[j].CurrentAvgDays {
			return trends[i].AvgDeltaDays > trends[j].AvgDeltaDays
		}
		return trends[i].CurrentAvgDays > trends[j].CurrentAvgDays
	})

	return LatencyTrendSummary{
		CurrentWindowStart: currentStart.Format(time.RFC3339),
		CurrentWindowEnd:   currentEnd.Format(time.RFC3339),
		PriorWindowStart:   priorStart.Format(time.RFC3339),
		PriorWindowEnd:     priorEnd.Format(time.RFC3339),
		WindowDays:         windowDays,
		Trends:             trends,
	}
}

func buildLatencyTrend(label string, current []float64, prior []float64) LatencyTrend {
	currentCount := len(current)
	priorCount := len(prior)

	currentAvg := average(current)
	priorAvg := average(prior)
	currentMedian := percentile(current, 50)
	priorMedian := percentile(prior, 50)

	avgDelta := currentAvg - priorAvg
	medianDelta := currentMedian - priorMedian

	avgDeltaPct := 0.0
	if priorAvg > 0 {
		avgDeltaPct = (avgDelta / priorAvg) * 100
	}
	medianDeltaPct := 0.0
	if priorMedian > 0 {
		medianDeltaPct = (medianDelta / priorMedian) * 100
	}

	trend := "flat"
	switch {
	case avgDelta > 0.5:
		trend = "up"
	case avgDelta < -0.5:
		trend = "down"
	}

	return LatencyTrend{
		Label:             label,
		CurrentCount:      currentCount,
		PriorCount:        priorCount,
		CurrentAvgDays:    round(currentAvg, 2),
		PriorAvgDays:      round(priorAvg, 2),
		AvgDeltaDays:      round(avgDelta, 2),
		AvgDeltaPercent:   round(avgDeltaPct, 1),
		CurrentMedianDays: round(currentMedian, 2),
		PriorMedianDays:   round(priorMedian, 2),
		MedianDeltaDays:   round(medianDelta, 2),
		MedianDeltaPct:    round(medianDeltaPct, 1),
		Trend:             trend,
	}
}

func flattenDurations(stageDurations map[string][]float64) []float64 {
	if len(stageDurations) == 0 {
		return nil
	}
	total := 0
	for _, durations := range stageDurations {
		total += len(durations)
	}
	out := make([]float64, 0, total)
	for _, durations := range stageDurations {
		out = append(out, durations...)
	}
	return out
}

func resolveAsOf(events []ReviewEvent, asOfInput string) (time.Time, error) {
	if asOfInput != "" {
		return parseDate(asOfInput)
	}
	if len(events) == 0 {
		return time.Time{}, errors.New("no events to resolve as-of date")
	}
	max := events[0].ReviewedAt
	for _, event := range events[1:] {
		if event.ReviewedAt.After(max) {
			max = event.ReviewedAt
		}
	}
	return max, nil
}

func buildQueueReport(queueItems []QueueItem, events []ReviewEvent, slaDays int, throughputDays int, asOf time.Time, dueSoonRatio float64) *QueueReport {
	if len(queueItems) == 0 {
		return nil
	}
	if dueSoonRatio <= 0 || dueSoonRatio >= 1 {
		dueSoonRatio = 0.8
	}
	dueSoonThreshold := float64(slaDays) * dueSoonRatio

	stageBuckets := map[string][]QueueItem{}
	reviewerBuckets := map[string][]QueueItem{}
	totalPending := len(queueItems)
	var totalAge float64
	overdue := 0
	dueSoon := 0
	onTrack := 0
	assignedCount := 0
	unassignedCount := 0

	for _, item := range queueItems {
		stageBuckets[item.Stage] = append(stageBuckets[item.Stage], item)
		reviewerID := strings.TrimSpace(item.ReviewerID)
		if reviewerID == "" {
			reviewerID = "unassigned"
			unassignedCount++
		} else {
			assignedCount++
		}
		reviewerBuckets[reviewerID] = append(reviewerBuckets[reviewerID], item)
		age := asOf.Sub(item.SubmittedAt).Hours() / 24
		if age < 0 {
			age = 0
		}
		totalAge += age
		switch {
		case age >= float64(slaDays):
			overdue++
		case age >= dueSoonThreshold:
			dueSoon++
		default:
			onTrack++
		}
	}

	stages := make([]QueueStageForecast, 0, len(stageBuckets))
	windowStart := asOf.AddDate(0, 0, -throughputDays)
	for stage, items := range stageBuckets {
		pending := len(items)
		var ageSum float64
		stageOverdue := 0
		stageDueSoon := 0
		stageOnTrack := 0
		for _, item := range items {
			age := asOf.Sub(item.SubmittedAt).Hours() / 24
			if age < 0 {
				age = 0
			}
			ageSum += age
			switch {
			case age >= float64(slaDays):
				stageOverdue++
			case age >= dueSoonThreshold:
				stageDueSoon++
			default:
				stageOnTrack++
			}
		}

		windowCount := 0
		for _, event := range events {
			if event.Stage != stage {
				continue
			}
			if !event.ReviewedAt.Before(windowStart) && !event.ReviewedAt.After(asOf) {
				windowCount++
			}
		}
		dailyThroughput := 0.0
		if throughputDays > 0 {
			dailyThroughput = float64(windowCount) / float64(throughputDays)
		}

		estimatedClear := 0.0
		clearanceStatus := "no throughput data"
		if dailyThroughput > 0 {
			estimatedClear = float64(pending) / dailyThroughput
			switch {
			case estimatedClear <= 7:
				clearanceStatus = "healthy"
			case estimatedClear <= 14:
				clearanceStatus = "watch"
			default:
				clearanceStatus = "at risk"
			}
		}

		avgAge := 0.0
		if pending > 0 {
			avgAge = ageSum / float64(pending)
		}

		stages = append(stages, QueueStageForecast{
			Stage:              stage,
			PendingCount:       pending,
			AvgAgeDays:         round(avgAge, 2),
			OverdueCount:       stageOverdue,
			DueSoonCount:       stageDueSoon,
			OnTrackCount:       stageOnTrack,
			DailyThroughput:    round(dailyThroughput, 2),
			EstimatedClearDays: round(estimatedClear, 2),
			ClearanceStatus:    clearanceStatus,
		})
	}

	sort.Slice(stages, func(i, j int) bool {
		if stages[i].PendingCount == stages[j].PendingCount {
			return stages[i].AvgAgeDays > stages[j].AvgAgeDays
		}
		return stages[i].PendingCount > stages[j].PendingCount
	})

	avgAge := 0.0
	if totalPending > 0 {
		avgAge = totalAge / float64(totalPending)
	}

	reviewerThroughput := map[string]int{}
	if throughputDays > 0 {
		for _, event := range events {
			if event.ReviewedAt.Before(windowStart) || event.ReviewedAt.After(asOf) {
				continue
			}
			reviewerID := strings.TrimSpace(event.ReviewerID)
			if reviewerID == "" {
				reviewerID = "unassigned"
			}
			reviewerThroughput[reviewerID]++
		}
	}
	reviewers := buildQueueReviewerForecasts(reviewerBuckets, reviewerThroughput, slaDays, throughputDays, asOf, dueSoonThreshold)

	return &QueueReport{
		AsOf:            asOf.Format(time.RFC3339),
		TotalPending:    totalPending,
		AssignedCount:   assignedCount,
		UnassignedCount: unassignedCount,
		OverdueCount:    overdue,
		DueSoonCount:    dueSoon,
		OnTrackCount:    onTrack,
		AvgAgeDays:      round(avgAge, 2),
		Stages:          stages,
		Reviewers:       reviewers,
		ThroughputDays:  throughputDays,
		DueSoonRatio:    dueSoonRatio,
	}
}

func buildQueueReviewerForecasts(reviewerBuckets map[string][]QueueItem, reviewerThroughput map[string]int, slaDays int, throughputDays int, asOf time.Time, dueSoonThreshold float64) []QueueReviewerForecast {
	if len(reviewerBuckets) == 0 {
		return nil
	}
	reviewers := make([]QueueReviewerForecast, 0, len(reviewerBuckets))
	for reviewerID, items := range reviewerBuckets {
		pending := len(items)
		var ageSum float64
		overdue := 0
		dueSoon := 0
		onTrack := 0
		for _, item := range items {
			age := asOf.Sub(item.SubmittedAt).Hours() / 24
			if age < 0 {
				age = 0
			}
			ageSum += age
			switch {
			case age >= float64(slaDays):
				overdue++
			case age >= dueSoonThreshold:
				dueSoon++
			default:
				onTrack++
			}
		}

		avgAge := 0.0
		if pending > 0 {
			avgAge = ageSum / float64(pending)
		}

		throughputPerWeek := 0.0
		if throughputDays > 0 {
			throughputPerWeek = float64(reviewerThroughput[reviewerID]) / (float64(throughputDays) / 7.0)
		}

		estimatedClear := 0.0
		clearanceStatus := "no throughput data"
		if throughputPerWeek > 0 {
			dailyThroughput := throughputPerWeek / 7.0
			estimatedClear = float64(pending) / dailyThroughput
			switch {
			case estimatedClear <= 7:
				clearanceStatus = "healthy"
			case estimatedClear <= 14:
				clearanceStatus = "watch"
			default:
				clearanceStatus = "at risk"
			}
		}

		reviewers = append(reviewers, QueueReviewerForecast{
			ReviewerID:         reviewerID,
			PendingCount:       pending,
			AvgAgeDays:         round(avgAge, 2),
			OverdueCount:       overdue,
			DueSoonCount:       dueSoon,
			OnTrackCount:       onTrack,
			ThroughputPerWeek:  round(throughputPerWeek, 2),
			EstimatedClearDays: round(estimatedClear, 2),
			ClearanceStatus:    clearanceStatus,
		})
	}

	sort.Slice(reviewers, func(i, j int) bool {
		if reviewers[i].PendingCount == reviewers[j].PendingCount {
			return reviewers[i].AvgAgeDays > reviewers[j].AvgAgeDays
		}
		return reviewers[i].PendingCount > reviewers[j].PendingCount
	})
	return reviewers
}

func buildReviewerStats(events []ReviewEvent, slaDays int, windowStart time.Time, asOf time.Time, throughputDays int) []ReviewerStats {
	reviewerBuckets := map[string][]ReviewEvent{}
	for _, event := range events {
		id := strings.TrimSpace(event.ReviewerID)
		if id == "" {
			id = "unassigned"
		}
		reviewerBuckets[id] = append(reviewerBuckets[id], event)
	}

	stats := make([]ReviewerStats, 0, len(reviewerBuckets))
	for reviewerID, bucket := range reviewerBuckets {
		if len(bucket) == 0 {
			continue
		}
		durations := make([]float64, 0, len(bucket))
		breachCount := 0
		lastReviewed := bucket[0].ReviewedAt
		buckets := AgingBuckets{}
		windowCount := 0
		for _, event := range bucket {
			days := event.ReviewedAt.Sub(event.SubmittedAt).Hours() / 24
			durations = append(durations, days)
			if days >= float64(slaDays) {
				breachCount++
			}
			switch {
			case days <= float64(slaDays):
				buckets.OnTime++
			case days <= float64(slaDays*2):
				buckets.AtRisk++
			default:
				buckets.Overdue++
			}
			if event.ReviewedAt.After(lastReviewed) {
				lastReviewed = event.ReviewedAt
			}
			if !event.ReviewedAt.Before(windowStart) && !event.ReviewedAt.After(asOf) {
				windowCount++
			}
		}

		sort.Float64s(durations)

		avg := average(durations)
		median := percentile(durations, 50)
		p90 := percentile(durations, 90)
		max := durations[len(durations)-1]
		breachRate := float64(breachCount) / float64(len(durations))
		riskTier := classifyRisk(avg, breachRate, slaDays)
		throughputPerWeek := float64(windowCount) / (float64(throughputDays) / 7.0)

		stats = append(stats, ReviewerStats{
			ReviewerID:        reviewerID,
			Count:             len(durations),
			AverageDays:       round(avg, 2),
			MedianDays:        round(median, 2),
			P90Days:           round(p90, 2),
			MaxDays:           round(max, 2),
			SLABreachCount:    breachCount,
			SLABreachRate:     round(breachRate*100, 1),
			LastReviewedAt:    lastReviewed.Format(time.RFC3339),
			ThroughputPerWeek: round(throughputPerWeek, 2),
			WindowCount:       windowCount,
			AgingBuckets:      buckets,
			RiskTier:          riskTier,
		})
	}

	sort.Slice(stats, func(i, j int) bool {
		if stats[i].ThroughputPerWeek == stats[j].ThroughputPerWeek {
			if stats[i].AverageDays == stats[j].AverageDays {
				return stats[i].Count > stats[j].Count
			}
			return stats[i].AverageDays > stats[j].AverageDays
		}
		return stats[i].ThroughputPerWeek > stats[j].ThroughputPerWeek
	})

	return stats
}

func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if len(values) == 1 {
		return values[0]
	}
	if p <= 0 {
		return values[0]
	}
	if p >= 100 {
		return values[len(values)-1]
	}

	rank := (p / 100) * float64(len(values)-1)
	lower := int(rank)
	upper := lower + 1
	if upper >= len(values) {
		return values[lower]
	}
	weight := rank - float64(lower)
	return values[lower] + (values[upper]-values[lower])*weight
}

func round(value float64, places int) float64 {
	factor := mathPow10(places)
	return float64(int(value*factor+0.5)) / factor
}

func percent(part, total int) float64 {
	if total == 0 {
		return 0
	}
	return round(float64(part)/float64(total)*100, 1)
}

func inWindow(value time.Time, start time.Time, end time.Time, includeEnd bool) bool {
	if value.Before(start) {
		return false
	}
	if includeEnd {
		return !value.After(end)
	}
	return value.Before(end)
}

func buildTrend(label string, current int, prior int, windowDays int) ThroughputTrend {
	delta := current - prior
	deltaPercent := 0.0
	if prior > 0 {
		deltaPercent = (float64(delta) / float64(prior)) * 100
	}
	currentPerWeek := 0.0
	priorPerWeek := 0.0
	if windowDays > 0 {
		currentPerWeek = float64(current) / (float64(windowDays) / 7.0)
		priorPerWeek = float64(prior) / (float64(windowDays) / 7.0)
	}
	trend := "flat"
	switch {
	case delta > 0:
		trend = "up"
	case delta < 0:
		trend = "down"
	}
	return ThroughputTrend{
		Label:          label,
		CurrentCount:   current,
		PriorCount:     prior,
		Delta:          delta,
		DeltaPercent:   round(deltaPercent, 1),
		CurrentPerWeek: round(currentPerWeek, 2),
		PriorPerWeek:   round(priorPerWeek, 2),
		Trend:          trend,
	}
}

func mathPow10(places int) float64 {
	if places <= 0 {
		return 1
	}
	result := 1.0
	for i := 0; i < places; i++ {
		result *= 10
	}
	return result
}

func writeCSVReports(report Report, output string) error {
	basePath, err := resolveCSVBase(output)
	if err != nil {
		return err
	}

	if err := writeStageCSV(basePath+"-stage-summary.csv", report.Stages); err != nil {
		return err
	}
	if err := writeReviewerCSV(basePath+"-reviewer-summary.csv", report.Reviewers); err != nil {
		return err
	}
	if err := writeThroughputCSV(basePath+"-throughput-summary.csv", report.Throughput); err != nil {
		return err
	}
	if err := writeTrendCSV(basePath+"-throughput-trend.csv", report.ThroughputTrend.Trends); err != nil {
		return err
	}
	if err := writeLatencyTrendCSV(basePath+"-latency-trend.csv", report.LatencyTrend.Trends); err != nil {
		return err
	}
	if report.Queue != nil {
		if err := writeQueueCSV(basePath+"-queue-forecast.csv", report.Queue); err != nil {
			return err
		}
		if err := writeQueueReviewerCSV(basePath+"-queue-reviewer-forecast.csv", report.Queue); err != nil {
			return err
		}
	}
	return nil
}

func resolveCSVBase(output string) (string, error) {
	output = strings.TrimSpace(output)
	if output == "" {
		return "", errors.New("csv output path is empty")
	}
	info, err := os.Stat(output)
	if err == nil && info.IsDir() {
		return filepath.Join(output, "review-queue"), nil
	}
	if err != nil && !os.IsNotExist(err) {
		return "", err
	}
	return strings.TrimSuffix(output, ".csv"), nil
}

func writeStageCSV(path string, stages []StageStats) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"stage", "count", "avg_days", "median_days", "p90_days", "max_days",
		"sla_breach_count", "sla_breach_rate", "distinct_reviewers",
		"on_time", "at_risk", "overdue", "risk_tier",
	}); err != nil {
		return err
	}
	for _, stats := range stages {
		record := []string{
			stats.Stage,
			strconv.Itoa(stats.Count),
			formatFloat(stats.AverageDays, 2),
			formatFloat(stats.MedianDays, 2),
			formatFloat(stats.P90Days, 2),
			formatFloat(stats.MaxDays, 2),
			strconv.Itoa(stats.SLABreachCount),
			formatFloat(stats.SLABreachRate, 1),
			strconv.Itoa(stats.DistinctReviewers),
			strconv.Itoa(stats.AgingBuckets.OnTime),
			strconv.Itoa(stats.AgingBuckets.AtRisk),
			strconv.Itoa(stats.AgingBuckets.Overdue),
			stats.RiskTier,
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeReviewerCSV(path string, reviewers []ReviewerStats) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"reviewer_id", "count", "avg_days", "median_days", "p90_days", "max_days",
		"sla_breach_count", "sla_breach_rate", "last_reviewed_at",
		"throughput_per_week", "window_count",
		"on_time", "at_risk", "overdue", "risk_tier",
	}); err != nil {
		return err
	}
	for _, stats := range reviewers {
		record := []string{
			stats.ReviewerID,
			strconv.Itoa(stats.Count),
			formatFloat(stats.AverageDays, 2),
			formatFloat(stats.MedianDays, 2),
			formatFloat(stats.P90Days, 2),
			formatFloat(stats.MaxDays, 2),
			strconv.Itoa(stats.SLABreachCount),
			formatFloat(stats.SLABreachRate, 1),
			stats.LastReviewedAt,
			formatFloat(stats.ThroughputPerWeek, 2),
			strconv.Itoa(stats.WindowCount),
			strconv.Itoa(stats.AgingBuckets.OnTime),
			strconv.Itoa(stats.AgingBuckets.AtRisk),
			strconv.Itoa(stats.AgingBuckets.Overdue),
			stats.RiskTier,
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeThroughputCSV(path string, throughput ThroughputSummary) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{"as_of", "window_days", "events_in_window", "throughput_per_week"}); err != nil {
		return err
	}
	record := []string{
		throughput.AsOf,
		strconv.Itoa(throughput.WindowDays),
		strconv.Itoa(throughput.EventsInWindow),
		formatFloat(throughput.ThroughputPerWeek, 2),
	}
	if err := writer.Write(record); err != nil {
		return err
	}
	writer.Flush()
	return writer.Error()
}

func writeTrendCSV(path string, trends []ThroughputTrend) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"label", "current_count", "prior_count", "delta", "delta_percent",
		"current_per_week", "prior_per_week", "trend",
	}); err != nil {
		return err
	}
	for _, trend := range trends {
		record := []string{
			trend.Label,
			strconv.Itoa(trend.CurrentCount),
			strconv.Itoa(trend.PriorCount),
			strconv.Itoa(trend.Delta),
			formatFloat(trend.DeltaPercent, 1),
			formatFloat(trend.CurrentPerWeek, 2),
			formatFloat(trend.PriorPerWeek, 2),
			trend.Trend,
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeQueueCSV(path string, queue *QueueReport) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"stage", "pending_count", "avg_age_days", "overdue_count", "due_soon_count",
		"on_track_count", "daily_throughput", "estimated_clear_days", "clearance_status",
		"assigned_count", "unassigned_count",
	}); err != nil {
		return err
	}
	overall := []string{
		"overall",
		strconv.Itoa(queue.TotalPending),
		formatFloat(queue.AvgAgeDays, 2),
		strconv.Itoa(queue.OverdueCount),
		strconv.Itoa(queue.DueSoonCount),
		strconv.Itoa(queue.OnTrackCount),
		"",
		"",
		"",
		strconv.Itoa(queue.AssignedCount),
		strconv.Itoa(queue.UnassignedCount),
	}
	if err := writer.Write(overall); err != nil {
		return err
	}
	for _, stage := range queue.Stages {
		record := []string{
			stage.Stage,
			strconv.Itoa(stage.PendingCount),
			formatFloat(stage.AvgAgeDays, 2),
			strconv.Itoa(stage.OverdueCount),
			strconv.Itoa(stage.DueSoonCount),
			strconv.Itoa(stage.OnTrackCount),
			formatFloat(stage.DailyThroughput, 2),
			formatFloat(stage.EstimatedClearDays, 2),
			stage.ClearanceStatus,
			"",
			"",
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeQueueReviewerCSV(path string, queue *QueueReport) error {
	if queue == nil || len(queue.Reviewers) == 0 {
		return nil
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"reviewer_id", "pending_count", "avg_age_days", "overdue_count", "due_soon_count",
		"on_track_count", "throughput_per_week", "estimated_clear_days", "clearance_status",
	}); err != nil {
		return err
	}
	for _, reviewer := range queue.Reviewers {
		record := []string{
			reviewer.ReviewerID,
			strconv.Itoa(reviewer.PendingCount),
			formatFloat(reviewer.AvgAgeDays, 2),
			strconv.Itoa(reviewer.OverdueCount),
			strconv.Itoa(reviewer.DueSoonCount),
			strconv.Itoa(reviewer.OnTrackCount),
			formatFloat(reviewer.ThroughputPerWeek, 2),
			formatFloat(reviewer.EstimatedClearDays, 2),
			reviewer.ClearanceStatus,
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func writeLatencyTrendCSV(path string, trends []LatencyTrend) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{
		"label", "current_count", "prior_count",
		"current_avg_days", "prior_avg_days", "avg_delta_days", "avg_delta_percent",
		"current_median_days", "prior_median_days", "median_delta_days", "median_delta_percent",
		"trend",
	}); err != nil {
		return err
	}
	for _, trend := range trends {
		record := []string{
			trend.Label,
			strconv.Itoa(trend.CurrentCount),
			strconv.Itoa(trend.PriorCount),
			formatFloat(trend.CurrentAvgDays, 2),
			formatFloat(trend.PriorAvgDays, 2),
			formatFloat(trend.AvgDeltaDays, 2),
			formatFloat(trend.AvgDeltaPercent, 1),
			formatFloat(trend.CurrentMedianDays, 2),
			formatFloat(trend.PriorMedianDays, 2),
			formatFloat(trend.MedianDeltaDays, 2),
			formatFloat(trend.MedianDeltaPct, 1),
			trend.Trend,
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func formatFloat(value float64, decimals int) string {
	return strconv.FormatFloat(value, 'f', decimals, 64)
}

func printReport(report Report, reviewerTop int) {
	fmt.Printf("Review Queue Forecaster\n")
	fmt.Printf("Generated: %s\n", report.GeneratedAt)
	fmt.Printf("SLA Days: %d\n", report.SLADays)
	fmt.Printf("Total Events: %d\n\n", report.TotalEvents)

	fmt.Println("Overall")
	printStats(report.Overall)
	fmt.Println()

	fmt.Println("By Stage")
	for _, stats := range report.Stages {
		printStats(stats)
	}

	fmt.Println()
	fmt.Println("Throughput")
	fmt.Printf("- Window: last %d days (as of %s)\n", report.Throughput.WindowDays, report.Throughput.AsOf)
	fmt.Printf("  Events in window: %d | Throughput: %.2f events/week\n", report.Throughput.EventsInWindow, report.Throughput.ThroughputPerWeek)

	printReviewerSnapshot(report.Reviewers, reviewerTop)
	printThroughputTrends(report.ThroughputTrend)
	printLatencyTrends(report.LatencyTrend)

	if report.Queue != nil {
		fmt.Println()
		fmt.Println("Queue Forecast")
		fmt.Printf("- As of %s | Pending: %d | Assigned: %d | Unassigned: %d | Avg Age: %.2f days\n",
			report.Queue.AsOf, report.Queue.TotalPending, report.Queue.AssignedCount, report.Queue.UnassignedCount, report.Queue.AvgAgeDays)
		fmt.Printf("  On Track: %d | Due Soon: %d | Overdue: %d | Due Soon Ratio: %.2f\n",
			report.Queue.OnTrackCount, report.Queue.DueSoonCount, report.Queue.OverdueCount, report.Queue.DueSoonRatio)
		for _, stage := range report.Queue.Stages {
			fmt.Printf("  - %s\n", stage.Stage)
			fmt.Printf("    Pending: %d | Avg Age: %.2f days | On Track: %d | Due Soon: %d | Overdue: %d\n",
				stage.PendingCount, stage.AvgAgeDays, stage.OnTrackCount, stage.DueSoonCount, stage.OverdueCount)
			fmt.Printf("    Daily Throughput: %.2f | Clear Days: %.2f | Status: %s\n",
				stage.DailyThroughput, stage.EstimatedClearDays, stage.ClearanceStatus)
		}
		if len(report.Queue.Reviewers) > 0 {
			maxReviewers := 5
			if maxReviewers > len(report.Queue.Reviewers) {
				maxReviewers = len(report.Queue.Reviewers)
			}
			fmt.Printf("  Reviewer Forecast (Top %d by Pending)\n", maxReviewers)
			for i := 0; i < maxReviewers; i++ {
				reviewer := report.Queue.Reviewers[i]
				fmt.Printf("  - %s\n", reviewer.ReviewerID)
				fmt.Printf("    Pending: %d | Avg Age: %.2f days | On Track: %d | Due Soon: %d | Overdue: %d\n",
					reviewer.PendingCount, reviewer.AvgAgeDays, reviewer.OnTrackCount, reviewer.DueSoonCount, reviewer.OverdueCount)
				fmt.Printf("    Throughput: %.2f/week | Clear Days: %.2f | Status: %s\n",
					reviewer.ThroughputPerWeek, reviewer.EstimatedClearDays, reviewer.ClearanceStatus)
			}
		}
	}
}

func printStats(stats StageStats) {
	fmt.Printf("- %s\n", stats.Stage)
	fmt.Printf("  Count: %d | Avg: %.2f days | Median: %.2f days | P90: %.2f days | Max: %.2f days\n",
		stats.Count, stats.AverageDays, stats.MedianDays, stats.P90Days, stats.MaxDays)
	fmt.Printf("  SLA Breach: %d (%.1f%%) | Distinct Reviewers: %d\n",
		stats.SLABreachCount, stats.SLABreachRate, stats.DistinctReviewers)
	if stats.Count > 0 {
		fmt.Printf("  Aging: On Time %d (%.1f%%) | At Risk %d (%.1f%%) | Overdue %d (%.1f%%) | Risk Tier: %s\n",
			stats.AgingBuckets.OnTime, percent(stats.AgingBuckets.OnTime, stats.Count),
			stats.AgingBuckets.AtRisk, percent(stats.AgingBuckets.AtRisk, stats.Count),
			stats.AgingBuckets.Overdue, percent(stats.AgingBuckets.Overdue, stats.Count),
			stats.RiskTier)
	}
}

func printReviewerSnapshot(reviewers []ReviewerStats, top int) {
	if len(reviewers) == 0 {
		return
	}
	if top <= 0 {
		top = 5
	}
	if top > len(reviewers) {
		top = len(reviewers)
	}

	fmt.Println()
	fmt.Printf("Reviewer Snapshot (Top %d by Throughput)\n", top)
	for i := 0; i < top; i++ {
		stats := reviewers[i]
		fmt.Printf("- %s\n", stats.ReviewerID)
		fmt.Printf("  Count: %d | Avg: %.2f days | Median: %.2f days | P90: %.2f days | Max: %.2f days\n",
			stats.Count, stats.AverageDays, stats.MedianDays, stats.P90Days, stats.MaxDays)
		fmt.Printf("  SLA Breach: %d (%.1f%%) | Last Reviewed: %s | Throughput: %.2f events/week\n",
			stats.SLABreachCount, stats.SLABreachRate, stats.LastReviewedAt, stats.ThroughputPerWeek)
		if stats.Count > 0 {
			fmt.Printf("  Aging: On Time %d (%.1f%%) | At Risk %d (%.1f%%) | Overdue %d (%.1f%%) | Risk Tier: %s\n",
				stats.AgingBuckets.OnTime, percent(stats.AgingBuckets.OnTime, stats.Count),
				stats.AgingBuckets.AtRisk, percent(stats.AgingBuckets.AtRisk, stats.Count),
				stats.AgingBuckets.Overdue, percent(stats.AgingBuckets.Overdue, stats.Count),
				stats.RiskTier)
		}
	}
}

func printThroughputTrends(summary ThroughputTrendSummary) {
	if len(summary.Trends) == 0 {
		return
	}
	maxStages := 5
	trends := summary.Trends

	fmt.Println()
	fmt.Println("Throughput Trend")
	fmt.Printf("- Current window: %s to %s (%d days)\n", summary.CurrentWindowStart, summary.CurrentWindowEnd, summary.WindowDays)
	fmt.Printf("  Prior window: %s to %s\n", summary.PriorWindowStart, summary.PriorWindowEnd)

	fmt.Println("  Overall")
	for _, trend := range trends {
		if trend.Label != "overall" {
			continue
		}
		fmt.Printf("  - %s | Current: %d | Prior: %d | Delta: %+d (%.1f%%) | Trend: %s\n",
			trend.Label, trend.CurrentCount, trend.PriorCount, trend.Delta, trend.DeltaPercent, trend.Trend)
		fmt.Printf("    Current: %.2f/week | Prior: %.2f/week\n", trend.CurrentPerWeek, trend.PriorPerWeek)
	}

	fmt.Printf("  Top %d Stages\n", maxStages)
	count := 0
	for _, trend := range trends {
		if trend.Label == "overall" {
			continue
		}
		fmt.Printf("  - %s | Current: %d | Prior: %d | Delta: %+d (%.1f%%) | Trend: %s\n",
			trend.Label, trend.CurrentCount, trend.PriorCount, trend.Delta, trend.DeltaPercent, trend.Trend)
		fmt.Printf("    Current: %.2f/week | Prior: %.2f/week\n", trend.CurrentPerWeek, trend.PriorPerWeek)
		count++
		if count >= maxStages {
			break
		}
	}
}

func printLatencyTrends(summary LatencyTrendSummary) {
	if len(summary.Trends) == 0 {
		return
	}
	maxStages := 5
	trends := summary.Trends

	fmt.Println()
	fmt.Println("Latency Trend")
	fmt.Printf("- Current window: %s to %s (%d days)\n", summary.CurrentWindowStart, summary.CurrentWindowEnd, summary.WindowDays)
	fmt.Printf("  Prior window: %s to %s\n", summary.PriorWindowStart, summary.PriorWindowEnd)

	fmt.Println("  Overall")
	for _, trend := range trends {
		if trend.Label != "overall" {
			continue
		}
		fmt.Printf("  - %s | Avg: %.2f -> %.2f days (%+.2f, %.1f%%) | Median: %.2f -> %.2f days (%+.2f, %.1f%%) | Trend: %s\n",
			trend.Label, trend.PriorAvgDays, trend.CurrentAvgDays, trend.AvgDeltaDays, trend.AvgDeltaPercent,
			trend.PriorMedianDays, trend.CurrentMedianDays, trend.MedianDeltaDays, trend.MedianDeltaPct, trend.Trend)
	}

	fmt.Printf("  Top %d Stages\n", maxStages)
	count := 0
	for _, trend := range trends {
		if trend.Label == "overall" {
			continue
		}
		fmt.Printf("  - %s | Avg: %.2f -> %.2f days (%+.2f, %.1f%%) | Median: %.2f -> %.2f days (%+.2f, %.1f%%) | Trend: %s\n",
			trend.Label, trend.PriorAvgDays, trend.CurrentAvgDays, trend.AvgDeltaDays, trend.AvgDeltaPercent,
			trend.PriorMedianDays, trend.CurrentMedianDays, trend.MedianDeltaDays, trend.MedianDeltaPct, trend.Trend)
		count++
		if count >= maxStages {
			break
		}
	}
}

func classifyRisk(avgDays float64, breachRate float64, slaDays int) string {
	sla := float64(slaDays)
	switch {
	case breachRate >= 0.4 || avgDays >= sla:
		return "high"
	case breachRate >= 0.2 || avgDays >= sla*0.8:
		return "medium"
	default:
		return "low"
	}
}

func init() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "\nCSV columns required: application_id, stage, submitted_at, reviewed_at, reviewer_id\n")
		fmt.Fprintf(flag.CommandLine.Output(), "Date formats accepted: RFC3339, YYYY-MM-DD, YYYY-MM-DD HH:MM:SS\n")
	}
}
