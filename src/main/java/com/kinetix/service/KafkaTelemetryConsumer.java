package com.kinetix.service;

import com.kinetix.model.IngestionEvent;
import org.springframework.stereotype.Service;
import java.util.logging.Logger;

@Service
public class KafkaTelemetryConsumer {

    private static final Logger log = Logger.getLogger(KafkaTelemetryConsumer.class.getName());
    private final ClickHouseNativeWriter clickHouseWriter;

    public KafkaTelemetryConsumer(ClickHouseNativeWriter clickHouseWriter) {
        this.clickHouseWriter = clickHouseWriter;
    }

    public void processTelemetryEvent(IngestionEvent event) {
        log.info(() -> String.format("Received Kafka telemetry event: id=%s, type=%s, risk=%.2f",
                event.getEventId(), event.getEventType(), event.getRiskScore()));
        
        clickHouseWriter.batchInsertEvents(java.util.List.of(event));
    }
}
