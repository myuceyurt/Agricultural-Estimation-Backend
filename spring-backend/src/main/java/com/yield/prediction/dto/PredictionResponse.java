package com.yield.prediction.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PredictionResponse(
    String status,
    PredictionData data
) {
    public record PredictionData(
        double lat,
        double lon,
        @JsonProperty("estimated_yield") String estimatedYield
    ) {}
}
