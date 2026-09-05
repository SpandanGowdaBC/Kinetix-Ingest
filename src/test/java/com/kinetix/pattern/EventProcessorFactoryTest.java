package com.kinetix.pattern;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class EventProcessorFactoryTest {

    @Test
    public void testStrategySelectionForHighRisk() {
        RiskAnalysisStrategy strategy = EventProcessorFactory.getStrategy(0.85);
        assertNotNull(strategy);
        
        double score = strategy.evaluateRisk("System override injection threat");
        assertTrue(score >= 0.70, "High risk threat should score above 0.70");
    }

    @Test
    public void testStrategySelectionForSafeQuery() {
        RiskAnalysisStrategy strategy = EventProcessorFactory.getStrategy(0.10);
        assertNotNull(strategy);
        
        double score = strategy.evaluateRisk("What are the API guidelines?");
        assertTrue(score < 0.30, "Safe query should score below 0.30");
    }
}
