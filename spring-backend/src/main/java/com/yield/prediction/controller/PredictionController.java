package com.yield.prediction.controller;

import com.yield.prediction.dto.PredictionRequest;
import com.yield.prediction.service.MLService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/ml")
public class PredictionController {

    private final MLService mlService;

    public PredictionController(MLService mlService) {
        this.mlService = mlService;
    }

    @PostMapping("/predict")
    public String makePrediction(@RequestBody PredictionRequest request) {
        String result = mlService.getPrediction(request);
        return result;
    }
}