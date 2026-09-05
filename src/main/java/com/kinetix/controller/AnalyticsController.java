package com.kinetix.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.time.Instant;
import java.util.*;

@RestController
@RequestMapping("/v1/analytics")
public class AnalyticsController {

    @GetMapping("/throughput")
    public ResponseEntity<Map<String, Object>> getIngestionThroughput() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("timestamp", Instant.now().toString());
        metrics.put("events_per_second", 2850);
        metrics.put("active_kafka_partitions", 12);
        metrics.put("clickhouse_batch_latency_ms", 1.85);
        metrics.put("total_ingested_today", 14820900);
        return ResponseEntity.ok(metrics);
    }

    @GetMapping("/threat-summary")
    public ResponseEntity<Map<String, Object>> getThreatSummary() {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("period", "24h");
        summary.put("total_blocked", 4820);
        summary.put("total_flagged", 12400);
        summary.put("top_threat_types", List.of("PROMPT_INJECTION", "PII_LEAK_EMAIL", "SECRET_KEY_EXFILTRATION"));
        return ResponseEntity.ok(summary);
    }
}
