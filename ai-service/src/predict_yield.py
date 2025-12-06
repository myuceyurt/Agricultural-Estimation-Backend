import pandas as pd
import numpy as np
import joblib
import os
import xgboost as xgb
from datetime import datetime
from gee.collect_point_data import collect_point_data
from solidgrids.get_soil_properties_for_point import get_soil_properties_for_point

MODEL_PATH = '/app/data/processed/konya_bugday_modeli_xgb.joblib'
if not os.path.exists(MODEL_PATH):
    MODEL_PATH = 'ai-service/data/processed/konya_bugday_modeli_xgb.joblib'

soilIncluded = True

REFERENCE_YEAR = 2025 

def predict_yield(lat, lon, hectare):

    print(f"\n🌍 ANALİZ BAŞLIYOR: {lat}, {lon} | {hectare} Hektar")
    
    print("📡 Uydu verileri taranıyor (Sentinel-2 & İklim)...")
    date_start = f"{REFERENCE_YEAR}-03-01"
    date_end = f"{REFERENCE_YEAR}-08-31"
    
    try:
        gee_df = collect_point_data(lon, lat, date_start, date_end, region_radius=500)
    except Exception as e:
        return {"error": f"GEE Bağlantı Hatası: {str(e)}"}

    if gee_df is None or gee_df.empty:
        return {"error": "Bu konum için uydu verisi bulunamadı (Deniz veya veri yok)."}

    print("🌱 Toprak analizi yapılıyor...")
    soil_df = get_soil_properties_for_point(lon, lat)
    

    if soil_df is not None:
        full_data = pd.concat([gee_df.reset_index(drop=True), soil_df.reset_index(drop=True)], axis=1)
    else:
        print("⚠️ Toprak verisi alınamadı, sadece uydu verisi kullanılıyor.")
        soilIncluded = False
        full_data = gee_df.copy()
        
    full_data['yil'] = REFERENCE_YEAR
    full_data['enlem'] = lat
    full_data['boylam'] = lon

    if not os.path.exists(MODEL_PATH):
        return {"error": f"Model dosyası bulunamadı: {MODEL_PATH}"}
    
    try:
        model = joblib.load(MODEL_PATH)
    except Exception as e:
        return {"error": f"Model yüklenemedi: {str(e)}"}

    try:
        model_features = model.feature_names_in_
    except AttributeError:
        return {"error": "Model özellik isimleri okunamadı."}

    for col in model_features:
        if col not in full_data.columns:
            full_data[col] = 0
            
    input_vector = full_data[model_features]

    try:
        prediction = model.predict(input_vector)[0]
        prediction = max(0.0, float(prediction))
    except Exception as e:
        return {"error": f"Tahmin hatası: {str(e)}"}

    total_ton = prediction * hectare

    return {
        "status": "success",
        "location": {"lat": lat, "lon": lon},
        "inputs": {
            "hectare": hectare,
            "reference_year": REFERENCE_YEAR
        },
        "results": {
            "yield_per_hektar": round(prediction, 3),
            "total_yield_ton": round(total_ton, 2)
        },
        "factors": {
            "elevation": round(full_data.get('elevation', [0])[0], 1),
            "rain_may": round(full_data.get('Rain_May', [0])[0], 1),
            "max_ndvi": round(full_data.get('NDVI_May', [0])[0], 2),
            "soil_included": soilIncluded
        }
    }

if __name__ == "__main__":
    test_lat = 38.65
    test_lon = 32.90

    print(f"Test çalıştırılıyor... (Model Yolu: {MODEL_PATH})")
    sonuc = predict_yield(test_lat, test_lon, 50)
    print("\nSONUÇ:")
    print(sonuc)