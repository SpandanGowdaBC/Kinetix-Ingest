package com.kinetix.pattern;

import com.kinetix.model.IngestionEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

public class EventProcessorFactoryTest {

    private EventProcessorFactory factory;

    @BeforeEach
    public void setUp() {
        DefaultRiskStrategy defaultStrategy = new DefaultRiskStrategy();
        factory = new EventProcessorFactory(Map.of("defaultRiskStrategy", defaultStrategy));
    }

    @Test
    public void testDefaultStrategySelection() {
        RiskAnalysisStrategy strategy = factory.getStrategy("UNKNOWN_EVENT");
        assertNotNull(strategy);

        IngestionEvent internalEvent = IngestionEvent.builder()
                .sourceIp("10.0.0.1")
                .eventType("UNKNOWN_EVENT")
                .build();

        double score = strategy.calculateRisk(internalEvent);
        assertEquals(0.05, score, 0.001, "Internal IP should evaluate to low risk score");
    }

    @Test
    public void testExternalIpStrategy() {
        RiskAnalysisStrategy strategy = factory.getStrategy("DEFAULT");
        assertNotNull(strategy);

        IngestionEvent externalEvent = IngestionEvent.builder()
                .sourceIp("192.168.1.1")
                .eventType("DEFAULT")
                .build();

        double score = strategy.calculateRisk(externalEvent);
        assertEquals(0.45, score, 0.001, "External IP should evaluate to default risk score");
    }
}

