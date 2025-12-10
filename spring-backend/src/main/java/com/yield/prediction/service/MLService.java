package com.yield.prediction.service;

import com.yield.prediction.dto.PredictionRequest;
import com.yield.prediction.dto.PredictionResponse;
import com.yield.prediction.model.Prediction;
import com.yield.prediction.repository.PredictionRepository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

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
    private final PredictionRepository predictionRepository;

    public MLService(
        WebClient.Builder builder, 
        @Value("${python.service.url}") String serviceUrl,
        PredictionRepository predictionRepository) {
        this.webClient = builder.baseUrl(serviceUrl).build();
        this.predictionRepository = predictionRepository;
    }

    public void deletePrediction(Long id) {
        logger.info("Deleting prediction with ID: {}", id);
        deleteFromDatabase(id);
    }

    public List<PredictionResponse> getAllPredictionsByDate() {
        logger.info("Fetching all predictions ordered by creation date");
        return getAllPredictions();
    }

    public PredictionResponse getPredictionById(Long id) {
        logger.info("Fetching prediction for ID: {}", id);
        return getPrediction(id);
    }

    public PredictionResponse startPrediction(PredictionRequest requestData) {
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
                saveToDatabase(requestData, response.data());
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

    private void saveToDatabase(PredictionRequest request, PredictionResponse.PredictionData data) {
        try {
            Prediction entity = new Prediction();
            
            entity.setLat(request.lat());
            entity.setLon(request.lon());
            entity.setHectare(request.hectare());
            
            double yieldVal = Double.parseDouble(data.totalYieldTon()); 
            entity.setTotalYieldTon(yieldVal);

            entity.setSoilIncluded(data.soilIncluded());

            entity.setYieldPerHectare(yieldVal / request.hectare());

            entity.setCreatedAt(LocalDateTime.now());

            predictionRepository.save(entity);
            
        } catch (Exception e) {
            logger.error("Failed to save prediction to database", e);
        }
    }

    private void deleteFromDatabase(Long id) {
        try {
            predictionRepository.deleteById(id);
        } catch (Exception e) {
            logger.error("Failed to delete prediction with ID: {}", id, e);
        }
    }

    private PredictionResponse getPrediction(Long id) {
        try {
            Prediction prediction = predictionRepository.findById(id).orElse(null);
            if (prediction == null) {
                logger.warn("Prediction with ID: {} not found", id);
                return new PredictionResponse("error", null);
            }
            return mapToResponse(prediction);
        } catch (Exception e) {
            logger.error("Failed to retrieve prediction with ID: {}", id, e);
            return new PredictionResponse("error", null);
        }
    }

    private List<PredictionResponse> getAllPredictions() {
        try {
            List<Prediction> predictions = predictionRepository.findAllByOrderByCreatedAtDesc();

            return predictions.stream()
                    .map(this::mapToResponse)
                    .collect(Collectors.toList());

        } catch (Exception e) {
            logger.error("Failed to retrieve predictions from database", e);
            return List.of();
        }
    }

    private PredictionResponse mapToResponse(Prediction prediction) {
        PredictionResponse.PredictionData data = new PredictionResponse.PredictionData(
            prediction.getId(),
            prediction.getLat(),
            prediction.getLon(),
            prediction.getHectare(),
            String.valueOf(prediction.getYieldPerHectare()),
            String.valueOf(prediction.getTotalYieldTon()),
            prediction.isSoilIncluded()
        );

        return new PredictionResponse("success", data);
}
}