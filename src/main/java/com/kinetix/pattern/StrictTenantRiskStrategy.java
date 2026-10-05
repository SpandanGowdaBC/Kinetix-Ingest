package com.kinetix.pattern;

import com.kinetix.model.IngestionEvent;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component("strictRiskStrategy")
public class StrictTenantRiskStrategy implements RiskAnalysisStrategy {

    @Override
    public double calculateRisk(IngestionEvent event) {
        if (event == null) {
            return 0.90;
        }

        if (event.getSourceIp() != null && event.getSourceIp().startsWith("10.")) {
            return 0.10;
        }

        Map<String, Object> payload = event.getPayload();
        if (payload != null) {
            if (Boolean.TRUE.equals(payload.get("prompt_injection")) || 
                "CRITICAL".equalsIgnoreCase(String.valueOf(payload.get("severity")))) {
                return 0.95;
            }
        }

        return 0.75;
    }
}
