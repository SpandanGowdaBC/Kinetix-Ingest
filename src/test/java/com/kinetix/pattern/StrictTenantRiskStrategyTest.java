package com.kinetix.pattern;

import com.kinetix.model.IngestionEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class StrictTenantRiskStrategyTest {

    private StrictTenantRiskStrategy strategy;

    @BeforeEach
    public void setUp() {
        strategy = new StrictTenantRiskStrategy();
    }

    @Test
    @DisplayName("Strict strategy should return low risk (0.10) for trusted internal subnet IPs")
    public void testTrustedSubnetIp() {
        IngestionEvent event = IngestionEvent.builder()
                .sourceIp("10.1.2.3")
                .build();

        assertEquals(0.10, strategy.calculateRisk(event), 0.001);
    }

    @Test
    @DisplayName("Strict strategy should return high risk (0.95) when prompt_injection flag is set")
    public void testPromptInjectionPayload() {
        IngestionEvent event = IngestionEvent.builder()
                .sourceIp("192.168.1.50")
                .payload(Map.of("prompt_injection", true))
                .build();

        assertEquals(0.95, strategy.calculateRisk(event), 0.001);
    }

    @Test
    @DisplayName("Strict strategy should return baseline external risk (0.75) for standard external IPs")
    public void testExternalIpBaseline() {
        IngestionEvent event = IngestionEvent.builder()
                .sourceIp("172.16.0.1")
                .build();

        assertEquals(0.75, strategy.calculateRisk(event), 0.001);
    }
}
