import sys
import os
from fastapi import FastAPI
from pydantic import BaseModel
import joblib
from src.predict_yield import predict_yield

sys.path.append(os.path.join(os.path.dirname(__file__), "src"))


app = FastAPI()

class PredictionRequest(BaseModel):
    lat: float
    lon: float
    hectare: float

@app.post("/predict")
def predict(request: PredictionRequest):
    
    yieldPrediction = predict_yield(request.lat, request.lon, request.hectare)    

    if "error" in yieldPrediction:
        return {
            "status": "error", 
            "message": yieldPrediction["error"],
            "debug": "Check container logs for more details"
        }
    
    return {
        "status": "success",
        "data": {
            "lat": request.lat,
            "lon": request.lon,
            "yield_per_hektar": yieldPrediction['results']['yield_per_hektar'],
            "total_yield_ton": yieldPrediction['results']['total_yield_ton'],
            "soil_included": yieldPrediction['factors']['soil_included']
        }
    }

@app.get("/")
def root():
    return {"message": "AI service is running"}
