package com.yield.prediction.dto;

public record PredictionRequest(
    double lat, 
    double lon, 
    double hectare
) {}