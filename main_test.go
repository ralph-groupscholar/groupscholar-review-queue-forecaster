package main

import "testing"

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
