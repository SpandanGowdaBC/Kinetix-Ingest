package com.kinetix.pattern;

import com.kinetix.model.IngestionEvent;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component("standardRiskStrategy")
public class StandardTenantRiskStrategy implements RiskAnalysisStrategy {

    @Override
    public double calculateRisk(IngestionEvent event) {
        if (event == null) {
            return 0.45;
        }

        if (event.getSourceIp() != null && event.getSourceIp().startsWith("10.")) {
            return 0.05;
        }

        Map<String, Object> payload = event.getPayload();
        if (payload != null) {
            if ("HIGH".equalsIgnoreCase(String.valueOf(payload.get("severity")))) {
                return 0.60;
            }
        }

        return 0.35;
    }
}
