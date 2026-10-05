package com.kinetix.service;

import com.kinetix.model.IngestionEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class ClickHouseNativeWriterTest {

    private NamedParameterJdbcTemplate namedJdbcTemplate;
    private ClickHouseNativeWriter writer;

    @BeforeEach
    public void setUp() {
        namedJdbcTemplate = mock(NamedParameterJdbcTemplate.class);
        writer = new ClickHouseNativeWriter(namedJdbcTemplate);
    }

    @Test
    @DisplayName("Should execute batchInsertEvents using NamedParameterJdbcTemplate")
    public void testBatchInsertEvents() {
        IngestionEvent event1 = IngestionEvent.builder()
                .eventId("evt_101")
                .tenantId("tenant-01")
                .eventType("AUTH_LOGIN")
                .sourceIp("10.0.0.1")
                .riskScore(0.05)
                .timestamp(System.currentTimeMillis())
                .build();

        IngestionEvent event2 = IngestionEvent.builder()
                .eventId("evt_102")
                .tenantId("tenant-02")
                .eventType("PAYMENT")
                .sourceIp("192.168.1.1")
                .riskScore(0.45)
                .timestamp(System.currentTimeMillis())
                .build();

        writer.batchInsertEvents(List.of(event1, event2));
        verify(namedJdbcTemplate, times(1)).batchUpdate(any(String.class), any(MapSqlParameterSource[].class));
    }

    @Test
    @DisplayName("Should query risk aggregations and return query result map")
    public void testQueryRiskAggregations() {
        when(namedJdbcTemplate.queryForList(any(String.class), any(Map.class)))
                .thenReturn(List.of(Map.of("event_type", "AUTH_LOGIN", "total_count", 150L, "avg_risk", 0.12)));

        List<Map<String, Object>> result = writer.queryRiskAggregations("tenant-01");
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("AUTH_LOGIN", result.get(0).get("event_type"));
    }

    @Test
    @DisplayName("Should return OFFLINE_FALLBACK when database exception occurs")
    public void testQueryRiskAggregationsErrorFallback() {
        when(namedJdbcTemplate.queryForList(any(String.class), any(Map.class)))
                .thenThrow(new RuntimeException("Connection refused"));

        List<Map<String, Object>> result = writer.queryRiskAggregations("tenant-offline");
        assertNotNull(result);
        assertEquals("OFFLINE_FALLBACK", result.get(0).get("status"));
    }
}
