import sys
import os
from fastapi import FastAPI
from pydantic import BaseModel
import joblib
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))


app = FastAPI()

MODEL_PATH = "" #TODO: Prediction model pathi eklenecek
model = None

@app.on_event("startup")
def load_model():
    global model
    if os.path.exists(MODEL_PATH):
        model = joblib.load(MODEL_PATH)
        print("Model yüklendi.")
    else:
        print(f"Model bulunamadı: {MODEL_PATH}")

class PredictionRequest(BaseModel):
    lat: float
    lon: float
    hectare: float

@app.post("/predict")
def predict(request: PredictionRequest):
    if not model:
        return {"status": "error", "message": "Model henüz eğitilmedi veya bulunamadı."}
    
    #TODO: Prediction burada yapılacak
    
    return {
        "status": "success",
        "data": {
            "lat": request.lat,
            "lon": request.lon,
            "estimated_yield": "TEST kg/hektar" 
        }
    }

@app.get("/")
def root():
    return {"message": "AI service is running"}
