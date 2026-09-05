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

    public RiskAnalysisStrategy getStrategy(String eventType) {
        return strategyMap.getOrDefault(eventType + "Strategy", strategyMap.get("defaultRiskStrategy"));
    }
}
