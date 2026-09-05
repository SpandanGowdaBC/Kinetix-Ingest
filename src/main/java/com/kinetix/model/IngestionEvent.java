package com.kinetix.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IngestionEvent {
    private String eventId;
    private String tenantId;
    private String eventType;
    private String sourceIp;
    private Map<String, Object> payload;
    private double riskScore;
    private long timestamp;
}
