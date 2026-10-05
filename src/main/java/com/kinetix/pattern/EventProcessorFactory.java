package com.kinetix.pattern;

import com.kinetix.model.IngestionEvent;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class EventProcessorFactory {

    private final Map<String, RiskAnalysisStrategy> strategyMap;

    public EventProcessorFactory(Map<String, RiskAnalysisStrategy> strategyMap) {
        this.strategyMap = strategyMap;
    }

    public RiskAnalysisStrategy getStrategy(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) {
            return strategyMap.getOrDefault("defaultRiskStrategy", new DefaultRiskStrategy());
        }

        String lowerTenant = tenantId.toLowerCase();
        if (lowerTenant.contains("strict") || lowerTenant.contains("security")) {
            return strategyMap.getOrDefault("strictRiskStrategy", strategyMap.getOrDefault("defaultRiskStrategy", new DefaultRiskStrategy()));
        } else if (lowerTenant.contains("standard") || lowerTenant.contains("acme")) {
            return strategyMap.getOrDefault("standardRiskStrategy", strategyMap.getOrDefault("defaultRiskStrategy", new DefaultRiskStrategy()));
        }

        RiskAnalysisStrategy strategy = strategyMap.get(tenantId + "Strategy");
        if (strategy != null) {
            return strategy;
        }

        strategy = strategyMap.get(tenantId);
        if (strategy != null) {
            return strategy;
        }

        return strategyMap.getOrDefault("defaultRiskStrategy", new DefaultRiskStrategy());
    }

    public int getStrategyCount() {
        return strategyMap.size();
    }
}
