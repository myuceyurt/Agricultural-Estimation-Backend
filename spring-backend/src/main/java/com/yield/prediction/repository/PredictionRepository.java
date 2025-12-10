package com.yield.prediction.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import com.yield.prediction.model.Prediction;

public interface PredictionRepository extends JpaRepository<Prediction, Long>{

    List<Prediction> findAllByOrderByCreatedAtDesc();
    
}