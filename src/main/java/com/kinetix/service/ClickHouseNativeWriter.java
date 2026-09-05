package com.kinetix.service;

import com.kinetix.model.IngestionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class ClickHouseNativeWriter {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseNativeWriter.class);
    private final NamedParameterJdbcTemplate namedJdbcTemplate;

    public ClickHouseNativeWriter(NamedParameterJdbcTemplate namedJdbcTemplate) {
        this.namedJdbcTemplate = namedJdbcTemplate;
    }

    /**
     * Executes native high-speed ClickHouse batch insert.
     */
    public void batchInsertEvents(List<IngestionEvent> events) {
        String sql = "INSERT INTO kinetix_db.risk_events " +
                     "(event_id, tenant_id, event_type, source_ip, risk_score, created_timestamp) " +
                     "VALUES (:eventId, :tenantId, :eventType, :sourceIp, :riskScore, :timestamp)";

        MapSqlParameterSource[] batchParams = events.stream()
                .map(event -> new MapSqlParameterSource()
                        .addValue("eventId", event.getEventId())
                        .addValue("tenantId", event.getTenantId())
                        .addValue("eventType", event.getEventType())
                        .addValue("sourceIp", event.getSourceIp())
                        .addValue("riskScore", event.getRiskScore())
                        .addValue("timestamp", event.getTimestamp()))
                .toArray(MapSqlParameterSource[]::new);

        try {
            namedJdbcTemplate.batchUpdate(sql, batchParams);
            log.info("[ClickHouse] Successfully batch-inserted {} records into ClickHouse", events.size());
        } catch (Exception e) {
            log.error("[ClickHouse Error] Failed batch insert: {}", e.getMessage(), e);
        }
    }

    /**
     * Executes analytical count aggregations natively against ClickHouse.
     */
    public List<Map<String, Object>> queryRiskAggregations(String tenantId) {
        String sql = "SELECT event_type, count() AS total_count, avg(risk_score) AS avg_risk " +
                     "FROM kinetix_db.risk_events " +
                     "WHERE tenant_id = :tenantId " +
                     "GROUP BY event_type ORDER BY total_count DESC LIMIT 10";

        Map<String, Object> params = Map.of("tenantId", tenantId);
        return namedJdbcTemplate.queryForList(sql, params);
    }
}
