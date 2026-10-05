package com.kinetix.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kinetix.model.IngestionEvent;
import com.kinetix.pattern.EventProcessorFactory;
import com.kinetix.pattern.RiskAnalysisStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

@Service
public class KafkaTelemetryConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaTelemetryConsumer.class);

    private final ClickHouseNativeWriter clickHouseWriter;
    private final EventProcessorFactory processorFactory;
    private final ObjectMapper objectMapper;

    private final ConcurrentLinkedQueue<IngestionEvent> eventBuffer = new ConcurrentLinkedQueue<>();

    @Value("${kinetix.ingestion.batch-size:1000}")
    private int batchSize = 1000;

    public KafkaTelemetryConsumer(ClickHouseNativeWriter clickHouseWriter,
                                  EventProcessorFactory processorFactory,
                                  ObjectMapper objectMapper) {
        this.clickHouseWriter = clickHouseWriter;
        this.processorFactory = processorFactory;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
        topics = "${kinetix.ingestion.topic-name:kinetix.risk.events}",
        groupId = "${spring.kafka.consumer.group-id:kinetix-ingest-group}"
    )
    public void onKafkaMessage(String message) {
        try {
            IngestionEvent event = objectMapper.readValue(message, IngestionEvent.class);
            processTelemetryEvent(event);
        } catch (Exception e) {
            log.error("[Kafka Consumer Error] Failed to parse message: {}", message, e);
        }
    }

    public void processTelemetryEvent(IngestionEvent event) {
        if (event.getEventId() == null || event.getEventId().isBlank()) {
            event.setEventId("evt_" + UUID.randomUUID().toString().substring(0, 8));
        }
        if (event.getTimestamp() <= 0) {
            event.setTimestamp(System.currentTimeMillis());
        }

        RiskAnalysisStrategy strategy = processorFactory.getStrategy(event.getTenantId());
        double calculatedRisk = strategy.calculateRisk(event);
        event.setRiskScore(calculatedRisk);

        eventBuffer.add(event);

        if (eventBuffer.size() >= batchSize) {
            flushBatch();
        }
    }

    @Scheduled(fixedDelayString = "${kinetix.ingestion.flush-interval-ms:500}")
    public synchronized void scheduledFlush() {
        if (!eventBuffer.isEmpty()) {
            flushBatch();
        }
    }

    public synchronized int flushBatch() {
        if (eventBuffer.isEmpty()) {
            return 0;
        }

        List<IngestionEvent> batchToInsert = new ArrayList<>();
        IngestionEvent event;
        while ((event = eventBuffer.poll()) != null && batchToInsert.size() < batchSize) {
            batchToInsert.add(event);
        }

        if (!batchToInsert.isEmpty()) {
            clickHouseWriter.batchInsertEvents(batchToInsert);
            log.info("[Kafka Batch Flush] Inserted batch of {} events into ClickHouse", batchToInsert.size());
        }
        return batchToInsert.size();
    }

    public int getBufferSize() {
        return eventBuffer.size();
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
