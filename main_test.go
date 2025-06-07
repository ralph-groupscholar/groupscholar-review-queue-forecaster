package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func findInsight(insights []Insight, area string) *Insight {
	for i := range insights {
		if insights[i].Area == area {
			return &insights[i]
		}
	}
	return nil
}

func TestBuildInsightsOverallRisk(t *testing.T) {
	overall := StageStats{
		Count:          10,
		AverageDays:    12.4,
		SLABreachRate:  35.0,
		RiskTier:       "high",
		Stage:          "overall",
		SLABreachCount: 3,
	}
	insights := buildInsights(overall, nil, ThroughputTrendSummary{}, LatencyTrendSummary{}, nil, 10)
	insight := findInsight(insights, "overall")
	if insight == nil {
		t.Fatalf("expected overall insight")
	}
	if insight.Severity != "high" {
		t.Fatalf("expected high severity, got %s", insight.Severity)
	}
}

func TestBuildInsightsQueueCoverage(t *testing.T) {
	queue := &QueueReport{
		TotalPending:    10,
		AssignedCount:   4,
		UnassignedCount: 6,
		OverdueCount:    0,
		DueSoonCount:    0,
	}
	insights := buildInsights(StageStats{}, nil, ThroughputTrendSummary{}, LatencyTrendSummary{}, queue, 10)
	insight := findInsight(insights, "coverage")
	if insight == nil {
		t.Fatalf("expected coverage insight")
	}
	if insight.Severity != "medium" {
		t.Fatalf("expected medium severity, got %s", insight.Severity)
	}
}

func TestBuildBriefIncludesQueueAndInsights(t *testing.T) {
	report := Report{
		GeneratedAt: "2026-02-07T12:00:00Z",
		TotalEvents: 12,
		SLADays:     10,
		Overall: StageStats{
			AverageDays:       9.2,
			MedianDays:        8.8,
			P90Days:           12.1,
			MaxDays:           14.0,
			SLABreachCount:    3,
			SLABreachRate:     25.0,
			RiskTier:          "medium",
			DistinctReviewers: 4,
		},
		Stages: []StageStats{
			{Stage: "initial", AverageDays: 11.2, SLABreachRate: 30.0, RiskTier: "high", Count: 4},
		},
		Insights: []Insight{
			{Severity: "medium", Area: "overall", Message: "SLA risk is trending up.", Metric: "breach 25.0%"},
		},
		Queue: &QueueReport{
			TotalPending:    5,
			AssignedCount:   3,
			UnassignedCount: 2,
			AvgAgeDays:      6.2,
			OnTrackCount:    2,
			DueSoonCount:    2,
			OverdueCount:    1,
			DueSoonRatio:    0.8,
			PriorityItems: []QueuePriorityItem{
				{ApplicationID: "A-100", Stage: "initial", ReviewerID: "rev-1", AgeDays: 9.1, Status: "due soon"},
			},
		},
	}
	content := buildBrief(report)
	if !strings.Contains(content, "Review Queue Ops Brief") {
		t.Fatalf("expected brief title")
	}
	if !strings.Contains(content, "Queue Snapshot") {
		t.Fatalf("expected queue snapshot section")
	}
	if !strings.Contains(content, "Insights") {
		t.Fatalf("expected insights section")
	}
	if !strings.Contains(content, "A-100") {
		t.Fatalf("expected priority item in brief")
	}
}

func TestWriteCSVReportsIncludesInsightsAndPriority(t *testing.T) {
	report := Report{
		GeneratedAt: time.Now().Format(time.RFC3339),
		TotalEvents: 1,
		Overall: StageStats{
			Stage:          "overall",
			AverageDays:    2.0,
			MedianDays:     2.0,
			P90Days:        2.0,
			MaxDays:        2.0,
			SLABreachCount: 0,
			SLABreachRate:  0,
			RiskTier:       "low",
		},
		Stages: []StageStats{{Stage: "review", Count: 1}},
		Reviewers: []ReviewerStats{{
			ReviewerID:        "rev-1",
			AverageDays:       2.0,
			ThroughputPerWeek: 1.0,
			SLABreachCount:    0,
			SLABreachRate:     0,
		}},
		SLADays: 10,
		Throughput: ThroughputSummary{
			AsOf:              time.Now().Format(time.RFC3339),
			WindowDays:        7,
			EventsInWindow:    1,
			ThroughputPerWeek: 1.0,
		},
		ThroughputTrend: ThroughputTrendSummary{
			Trends: []ThroughputTrend{{Label: "overall"}},
		},
		LatencyTrend: LatencyTrendSummary{
			Trends: []LatencyTrend{{Label: "overall"}},
		},
		Insights: []Insight{{Severity: "high", Area: "overall", Message: "test", Metric: "metric"}},
		Queue: &QueueReport{
			TotalPending: 1,
			PriorityItems: []QueuePriorityItem{{
				ApplicationID: "app-1",
				Stage:         "review",
				ReviewerID:    "rev-1",
				SubmittedAt:   time.Now().Format(time.RFC3339),
				AgeDays:       2.0,
				DaysToSLA:     8.0,
				UrgencyScore:  1.2,
				Status:        "due soon",
			}},
		},
	}

	dir := t.TempDir()
	if err := writeCSVReports(report, dir); err != nil {
		t.Fatalf("writeCSVReports failed: %v", err)
	}

	base := filepath.Join(dir, "review-queue")
	paths := []string{
		base + "-insights.csv",
		base + "-queue-priority.csv",
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected csv output at %s: %v", path, err)
		}
	}
}
