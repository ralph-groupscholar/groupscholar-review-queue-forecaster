package main

import (
	"strings"
	"testing"
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
