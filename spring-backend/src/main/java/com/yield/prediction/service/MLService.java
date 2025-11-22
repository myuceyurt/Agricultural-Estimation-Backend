package com.yield.prediction.service;

import com.yield.prediction.dto.PredictionRequest;
import com.yield.prediction.dto.PredictionResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Service
public class MLService {

    private final WebClient webClient;

    public MLService(WebClient.Builder builder, @Value("${python.service.url}") String serviceUrl) {
        this.webClient = builder.baseUrl(serviceUrl).build();
    }

    public String getPrediction(PredictionRequest requestData) {
        PredictionResponse response = webClient.post()
                .uri("/predict")
                .bodyValue(requestData)
                .retrieve()
                .bodyToMono(PredictionResponse.class)
                .block();

        if (response != null && "success".equals(response.status())) {
            return response.data().estimatedYield();
        } else {
            return "Error: Unable to get prediction";
        }
    }
}
