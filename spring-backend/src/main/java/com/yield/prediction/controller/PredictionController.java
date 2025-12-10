package com.yield.prediction.controller;

import com.yield.prediction.dto.PredictionRequest;
import com.yield.prediction.dto.PredictionResponse;
import com.yield.prediction.service.MLService;

import java.util.List;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/ml")
public class PredictionController {

    private final MLService mlService;

    public PredictionController(MLService mlService) {
        this.mlService = mlService;
    }

    @PostMapping("/predict")
    public PredictionResponse makePrediction(@RequestBody PredictionRequest request) {
        return mlService.startPrediction(request);
    }

    @DeleteMapping("/predictions/delete/{id}")
    public void deletePrediction(@PathVariable Long id) {
        mlService.deletePrediction(id);
    }

    @GetMapping("/predictions/createdAt")
    public List<PredictionResponse> getAllPredictionsByDate() {
        return mlService.getAllPredictionsByDate();
    }

    @GetMapping("/predictions/{id}")
    public PredictionResponse getPredictionById(@PathVariable Long id) {
        return mlService.getPredictionById(id);
    }
}