package com.kinetix.pattern;

import com.kinetix.model.IngestionEvent;

public interface RiskAnalysisStrategy {
    double calculateRisk(IngestionEvent event);
}
