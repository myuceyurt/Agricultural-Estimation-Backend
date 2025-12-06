package com.yield.prediction.service;

import com.yield.prediction.dto.PredictionRequest;
import com.yield.prediction.dto.PredictionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

@Service
public class MLService {

    private static final Logger logger = LoggerFactory.getLogger(MLService.class);
    private final WebClient webClient;

    public MLService(WebClient.Builder builder, @Value("${python.service.url}") String serviceUrl) {
        this.webClient = builder.baseUrl(serviceUrl).build();
    }

    public PredictionResponse getPrediction(PredictionRequest requestData) {
        logger.info("Initiating prediction request for Lat: {}, Lon: {}, Hectare: {}", 
                    requestData.lat(), requestData.lon(), requestData.hectare());

        try {
            PredictionResponse response = webClient.post()
                    .uri("/predict")
                    .bodyValue(requestData)
                    .retrieve()
                    .bodyToMono(PredictionResponse.class)
                    .block();

            if (response == null) {
                logger.error("Received null response from AI service");
                return new PredictionResponse("error", null);
            }

            if ("success".equalsIgnoreCase(response.status())) {
                logger.info("Prediction successful. Total yield in ton: {}", response.data().totalYieldTon());
                return new PredictionResponse("success", response.data());
            } else {
                logger.warn("AI Service returned failure status: {}", response.status());
                return new PredictionResponse("error", null);
            }

        } catch (WebClientResponseException e) {
            logger.error("AI Service returned HTTP Error. Status: {}, Body: {}", e.getStatusCode(), e.getResponseBodyAsString());
            return new PredictionResponse("error", null);

        } catch (WebClientRequestException e) {
            logger.error("Connection failed. Could not reach AI Service. Reason: {}", e.getMessage());
            return new PredictionResponse("error", null);

        } catch (Exception e) {
            logger.error("Unexpected error during prediction", e);
            return new PredictionResponse("error", null);
        }
    }
}