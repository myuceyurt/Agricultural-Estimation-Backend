package com.yield.prediction.model;

import java.time.LocalDateTime;

import jakarta.persistence.*;

@Entity
@Table(name = "predictions")
public class Prediction {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "latitude", nullable = false)
    private double lat;

    @Column(name = "longitude", nullable = false)
    private double lon;

    @Column(name = "hectare", nullable = false)
    private double hectare;

    @Column(name = "yield_per_hektar", nullable = false)
    private double yieldPerHektar;

    @Column(name = "total_yield_ton", nullable = false)
    private double totalYieldTon;

    @Column(name = "soil_included", nullable = false)
    private boolean soilIncluded;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    public Prediction() {}

    public Prediction(double lat, double lon, double hectare, double yieldPerHektar, double totalYieldTon, boolean soilIncluded, LocalDateTime createdAt) {
        this.lat = lat;
        this.lon = lon;
        this.hectare = hectare;
        this.yieldPerHektar = yieldPerHektar;
        this.totalYieldTon = totalYieldTon;
        this.soilIncluded = soilIncluded;
        this.createdAt = createdAt;
    }

    public Long getId() {
        return id;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getHectare() {
        return hectare;
    }

    public void setHectare(double hectare) {
        this.hectare = hectare;
    }

    public double getYieldPerHectare() {
        return yieldPerHektar;
    }

    public void setYieldPerHectare(double yieldPerHektar) {
        this.yieldPerHektar = yieldPerHektar;
    }

    public double getTotalYieldTon() {
        return totalYieldTon;
    }

    public void setTotalYieldTon(double totalYieldTon) {
        this.totalYieldTon = totalYieldTon;
    }

    public boolean isSoilIncluded() {
        return soilIncluded;
    }

    public void setSoilIncluded(boolean soilIncluded) {
        this.soilIncluded = soilIncluded;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

}
