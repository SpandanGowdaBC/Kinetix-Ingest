package com.kinetix.pattern;

import com.kinetix.model.IngestionEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class StandardTenantRiskStrategyTest {

    private StandardTenantRiskStrategy strategy;

    @BeforeEach
    public void setUp() {
        strategy = new StandardTenantRiskStrategy();
    }

    @Test
    @DisplayName("Standard strategy should return low risk (0.05) for internal subnet IPs")
    public void testTrustedSubnetIp() {
        IngestionEvent event = IngestionEvent.builder()
                .sourceIp("10.0.0.100")
                .build();

        assertEquals(0.05, strategy.calculateRisk(event), 0.001);
    }

    @Test
    @DisplayName("Standard strategy should return elevated risk (0.60) for HIGH severity payloads")
    public void testHighSeverityPayload() {
        IngestionEvent event = IngestionEvent.builder()
                .sourceIp("192.168.1.10")
                .payload(Map.of("severity", "HIGH"))
                .build();

        assertEquals(0.60, strategy.calculateRisk(event), 0.001);
    }
}
