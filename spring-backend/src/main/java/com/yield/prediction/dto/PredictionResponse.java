package com.yield.prediction.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PredictionResponse(
    String status,
    PredictionData data
) {
    public record PredictionData(
        Long id,
        Double lat,
        Double lon,
        Double hectare,
        @JsonProperty("yield_per_hektar") String yieldPerHektar,
        @JsonProperty("total_yield_ton") String totalYieldTon,
        @JsonProperty("soil_included") boolean soilIncluded
    ) {}
}
