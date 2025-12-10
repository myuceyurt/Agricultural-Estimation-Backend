package com.yield.prediction.service;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.web.reactive.function.client.WebClient;

import com.yield.prediction.dto.PredictionRequest;
import com.yield.prediction.dto.PredictionResponse;
import com.yield.prediction.repository.PredictionRepository;

class MLServiceTest {
    private MockWebServer mockWebServer;
    private MLService mlService;

    @BeforeEach
    void setUp() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();

        String fakeBaseUrl = mockWebServer.url("/").toString();
        WebClient.Builder webClientBuilder = WebClient.builder();
        PredictionRepository fakeRepository = null;


        mlService = new MLService(webClientBuilder, fakeBaseUrl, fakeRepository);
    }

    @AfterEach
    void tearDown() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    void testGetPredictionSuccess(){
        MockResponse mockResponse = new MockResponse()
                .setResponseCode(200)
                .setHeader("Content-Type", "application/json")
                .setBody("""
                    {
                        "status": "success",
                        "data": {
                            "lat": 41.0,
                            "lon": 28.0,
                            "estimated_yield": "1500 kg/hektar"
                        }
                    }
                """);

        mockWebServer.enqueue(mockResponse);
        PredictionRequest request = new PredictionRequest(41.025813, 28.889179, 10.0);
        PredictionResponse result = mlService.getPrediction(request);

        assert result.data().yieldPerHektar().equals("1500 kg/hektar");
    }

    @Test
    void testGetPredictionFailure(){
        MockResponse mockResponse = new MockResponse()
                .setResponseCode(500);

        mockWebServer.enqueue(mockResponse);
        PredictionRequest request = new PredictionRequest(41.025813, 28.889179, 10.0);

        assertThrows(Exception.class, () -> {
            mlService.getPrediction(request);
        });
    }
}