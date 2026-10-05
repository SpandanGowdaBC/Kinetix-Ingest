package com.kinetix.pattern;

import com.kinetix.model.IngestionEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class EventProcessorFactoryTest {

    private EventProcessorFactory factory;

    @BeforeEach
    public void setUp() {
        DefaultRiskStrategy defaultStrategy = new DefaultRiskStrategy();
        StrictTenantRiskStrategy strictStrategy = new StrictTenantRiskStrategy();
        StandardTenantRiskStrategy standardStrategy = new StandardTenantRiskStrategy();

        factory = new EventProcessorFactory(Map.of(
                "defaultRiskStrategy", defaultStrategy,
                "strictRiskStrategy", strictStrategy,
                "standardRiskStrategy", standardStrategy
        ));
    }

    @Test
    @DisplayName("Should select StrictTenantRiskStrategy when tenant contains 'strict'")
    public void testStrictTenantStrategySelection() {
        RiskAnalysisStrategy strategy = factory.getStrategy("tenant-strict-99");
        assertTrue(strategy instanceof StrictTenantRiskStrategy);

        IngestionEvent event = IngestionEvent.builder()
                .tenantId("tenant-strict-99")
                .sourceIp("192.168.1.1")
                .build();

        double risk = strategy.calculateRisk(event);
        assertEquals(0.75, risk, 0.001);
    }

    @Test
    @DisplayName("Should select StandardTenantRiskStrategy when tenant contains 'standard' or 'acme'")
    public void testStandardTenantStrategySelection() {
        RiskAnalysisStrategy strategy = factory.getStrategy("tenant-acme-corp");
        assertTrue(strategy instanceof StandardTenantRiskStrategy);

        IngestionEvent event = IngestionEvent.builder()
                .tenantId("tenant-acme-corp")
                .sourceIp("192.168.1.1")
                .build();

        double risk = strategy.calculateRisk(event);
        assertEquals(0.35, risk, 0.001);
    }

    @Test
    @DisplayName("Should fallback to DefaultRiskStrategy for unknown tenant IDs")
    public void testUnknownTenantFallback() {
        RiskAnalysisStrategy strategy = factory.getStrategy("unknown-tenant-xyz");
        assertTrue(strategy instanceof DefaultRiskStrategy);

        IngestionEvent event = IngestionEvent.builder()
                .tenantId("unknown-tenant-xyz")
                .sourceIp("10.0.0.5")
                .build();

        double risk = strategy.calculateRisk(event);
        assertEquals(0.05, risk, 0.001);
    }

    @Test
    @DisplayName("Should fallback to DefaultRiskStrategy when tenant ID is null or blank")
    public void testNullTenantFallback() {
        RiskAnalysisStrategy nullStrategy = factory.getStrategy(null);
        assertTrue(nullStrategy instanceof DefaultRiskStrategy);

        RiskAnalysisStrategy blankStrategy = factory.getStrategy("   ");
        assertTrue(blankStrategy instanceof DefaultRiskStrategy);
    }
}
