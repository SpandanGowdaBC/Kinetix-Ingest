package com.kinetix.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kinetix.model.IngestionEvent;
import com.kinetix.pattern.DefaultRiskStrategy;
import com.kinetix.pattern.EventProcessorFactory;
import com.kinetix.pattern.StandardTenantRiskStrategy;
import com.kinetix.pattern.StrictTenantRiskStrategy;
import com.kinetix.service.ClickHouseNativeWriter;
import com.kinetix.service.KafkaTelemetryConsumer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

public class KinetixBenchmarkRunnerTest {

    @Test
    @DisplayName("Run 100,000 Event In-Memory Micro-Benchmark (Mocked ClickHouse)")
    public void runIngestionThroughputBenchmark() {
        NamedParameterJdbcTemplate mockJdbc = mock(NamedParameterJdbcTemplate.class);
        ClickHouseNativeWriter writer = new ClickHouseNativeWriter(mockJdbc);

        EventProcessorFactory factory = new EventProcessorFactory(Map.of(
                "defaultRiskStrategy", new DefaultRiskStrategy(),
                "strictRiskStrategy", new StrictTenantRiskStrategy(),
                "standardRiskStrategy", new StandardTenantRiskStrategy()
        ));

        KafkaTelemetryConsumer consumer = new KafkaTelemetryConsumer(writer, factory, new ObjectMapper());
        consumer.setBatchSize(1000);

        int totalEvents = 100000;
        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= totalEvents; i++) {
            String tenantId = (i % 3 == 0) ? "tenant-strict-99" : (i % 2 == 0) ? "tenant-acme-standard" : "tenant-default";
            String ip = (i % 5 == 0) ? "10.0.0." + (i % 250) : "192.168.1." + (i % 250);

            IngestionEvent event = IngestionEvent.builder()
                    .eventId("evt_" + i)
                    .tenantId(tenantId)
                    .eventType(i % 2 == 0 ? "AUTH_LOGIN" : "PAYMENT")
                    .sourceIp(ip)
                    .payload(Map.of("index", i, "prompt_injection", i % 1000 == 0))
                    .timestamp(System.currentTimeMillis())
                    .build();

            consumer.processTelemetryEvent(event);
        }

        // Flush any remaining in buffer
        consumer.scheduledFlush();

        long durationMs = System.currentTimeMillis() - startTime;
        double seconds = durationMs / 1000.0;
        double eventsPerSecond = totalEvents / (seconds > 0 ? seconds : 0.001);
        double avgBatchLatencyMs = (double) durationMs / (totalEvents / 1000.0);

        System.out.println("=========================================================");
        System.out.println("🔥 IN-MEMORY MICRO-BENCHMARK RESULT (MOCKED CLICKHOUSE WRITER) 🔥");
        System.out.println("---------------------------------------------------------");
        System.out.println("Total Events Processed     : " + totalEvents);
        System.out.println("Total Benchmark Time       : " + durationMs + " ms (" + String.format("%.3f", seconds) + " seconds)");
        System.out.println("In-Memory Rate (Mocked DB) : " + String.format("%.2f", eventsPerSecond) + " events/sec");
        System.out.println("Average In-Memory Batch Overhead: " + String.format("%.2f", avgBatchLatencyMs) + " ms/batch (1000 records)");
        System.out.println("=========================================================");
    }
}
