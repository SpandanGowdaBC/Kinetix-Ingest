package com.kinetix.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kinetix.model.IngestionEvent;
import com.kinetix.pattern.DefaultRiskStrategy;
import com.kinetix.pattern.EventProcessorFactory;
import com.kinetix.pattern.StrictTenantRiskStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

public class KafkaTelemetryConsumerTest {

    private ClickHouseNativeWriter clickHouseWriter;
    private EventProcessorFactory processorFactory;
    private ObjectMapper objectMapper;
    private KafkaTelemetryConsumer consumer;

    @BeforeEach
    public void setUp() {
        clickHouseWriter = mock(ClickHouseNativeWriter.class);
        processorFactory = new EventProcessorFactory(Map.of(
                "defaultRiskStrategy", new DefaultRiskStrategy(),
                "strictRiskStrategy", new StrictTenantRiskStrategy()
        ));
        objectMapper = new ObjectMapper();

        consumer = new KafkaTelemetryConsumer(clickHouseWriter, processorFactory, objectMapper);
        consumer.setBatchSize(3); // Small batch size for test
    }

    @Test
    @DisplayName("Should buffer events and flush batch when batch size threshold is reached")
    public void testBatchBufferingAndFlushThreshold() {
        IngestionEvent event1 = IngestionEvent.builder().tenantId("tenant-strict").sourceIp("10.0.0.1").build();
        IngestionEvent event2 = IngestionEvent.builder().tenantId("tenant-strict").sourceIp("192.168.1.1").build();
        IngestionEvent event3 = IngestionEvent.builder().tenantId("tenant-strict").sourceIp("192.168.1.2").build();

        consumer.processTelemetryEvent(event1);
        consumer.processTelemetryEvent(event2);
        assertEquals(2, consumer.getBufferSize());
        verify(clickHouseWriter, never()).batchInsertEvents(anyList());

        consumer.processTelemetryEvent(event3); // Hits batch size threshold of 3
        assertEquals(0, consumer.getBufferSize());
        verify(clickHouseWriter, times(1)).batchInsertEvents(anyList());
    }

    @Test
    @DisplayName("Should flush buffer during scheduled time-limit flush")
    public void testScheduledFlush() {
        IngestionEvent event = IngestionEvent.builder().tenantId("tenant-standard").sourceIp("10.0.0.1").build();
        consumer.processTelemetryEvent(event);
        assertEquals(1, consumer.getBufferSize());

        consumer.scheduledFlush();
        assertEquals(0, consumer.getBufferSize());
        verify(clickHouseWriter, times(1)).batchInsertEvents(anyList());
    }

    @Test
    @DisplayName("Should parse Kafka JSON payload message and process telemetry event")
    public void testOnKafkaMessageParsing() {
        String jsonMessage = "{\"eventId\":\"evt_100\",\"tenantId\":\"tenant-strict\",\"eventType\":\"AUTH_LOGIN\",\"sourceIp\":\"10.0.0.1\"}";
        consumer.onKafkaMessage(jsonMessage);

        assertEquals(1, consumer.getBufferSize());
        consumer.scheduledFlush();
        verify(clickHouseWriter, times(1)).batchInsertEvents(anyList());
    }

    @Test
    @DisplayName("Should ensure zero event loss when flushing batches larger than batch size threshold")
    public void testZeroEventLossDuringBatchFlush() {
        consumer.setBatchSize(3);

        // Process 5 events (3 should trigger automatic flush, 2 remain in buffer)
        for (int i = 1; i <= 5; i++) {
            IngestionEvent event = IngestionEvent.builder()
                    .eventId("evt_" + i)
                    .tenantId("tenant-standard")
                    .sourceIp("10.0.0." + i)
                    .build();
            consumer.processTelemetryEvent(event);
        }

        // Verify batch 1 of 3 items was flushed automatically
        verify(clickHouseWriter, times(1)).batchInsertEvents(anyList());
        assertEquals(2, consumer.getBufferSize(), "Remaining 2 events must stay in buffer without being dropped");

        // Flush remaining 2 items
        consumer.scheduledFlush();
        assertEquals(0, consumer.getBufferSize());
        verify(clickHouseWriter, times(2)).batchInsertEvents(anyList());
    }
}
