package com.kinetix.controller;

import com.kinetix.model.IngestionEvent;
import com.kinetix.pattern.EventProcessorFactory;
import com.kinetix.pattern.RiskAnalysisStrategy;
import com.kinetix.service.ClickHouseNativeWriter;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/ingest")
public class IngestionController {

    private final ClickHouseNativeWriter clickHouseWriter;
    private final EventProcessorFactory processorFactory;

    public IngestionController(ClickHouseNativeWriter clickHouseWriter, EventProcessorFactory processorFactory) {
        this.clickHouseWriter = clickHouseWriter;
        this.processorFactory = processorFactory;
    }

    @PostMapping("/events")
    public ResponseEntity<Map<String, Object>> ingestEvents(@RequestBody List<IngestionEvent> events) {
        for (IngestionEvent event : events) {
            RiskAnalysisStrategy strategy = processorFactory.getStrategy(event.getEventType());
            event.setRiskScore(strategy.calculateRisk(event));
        }

        clickHouseWriter.batchInsertEvents(events);

        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "ingested_count", events.size(),
                "timestamp", System.currentTimeMillis()
        ));
    }

    @GetMapping("/analytics/{tenantId}")
    public ResponseEntity<List<Map<String, Object>>> getAnalytics(@PathVariable String tenantId) {
        List<Map<String, Object>> analytics = clickHouseWriter.queryRiskAggregations(tenantId);
        return ResponseEntity.ok(analytics);
    }
}
