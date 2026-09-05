package com.kinetix.pattern;

import com.kinetix.model.IngestionEvent;
import org.springframework.stereotype.Component;

@Component("defaultRiskStrategy")
public class DefaultRiskStrategy implements RiskAnalysisStrategy {

    @Override
    public double calculateRisk(IngestionEvent event) {
        if (event.getSourceIp() != null && event.getSourceIp().startsWith("10.")) {
            return 0.05; // Internal trusted subnet
        }
        return 0.45;
    }
}
