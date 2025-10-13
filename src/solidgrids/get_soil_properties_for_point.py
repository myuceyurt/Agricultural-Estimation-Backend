import json
import requests
import pandas as pd

def get_soil_properties_for_point(lon, lat):

    print(f"Analiz noktası: Enlem={lat}, Boylam={lon}")
    print("SoilGrids RESTful API'sine istek gönderiliyor...")

    BASE_URL = "https://rest.isric.org/soilgrids/v2.0/properties/query"
    params = {
        'lon': lon,
        'lat': lat,
        'property': ["clay", "sand", "silt", "phh2o", "cec", "soc"],
        'depth': ["0-5cm", "5-15cm", "15-30cm"],
        'value': ["mean"]
    }

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        print("Veriler başarıyla çekildi. Yanıt işleniyor...")
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP Hatası Oluştu: {http_err}\n   - Sunucu Cevabı: {response.text}")
        return None
    except Exception as e:
        print(f"Hata oluştu: {e}")
        return None

    processed_data = []
    
    properties = data.get('properties', {})
    layers = properties.get('layers', [])

    for layer in layers:
        prop_name = layer.get('name', 'N/A')
        unit_measure = layer.get('unit_measure', {})
        unit = unit_measure.get('d_symbol', 'N/A')
        
        for depth_info in layer.get('depths', []):
            depth_label = depth_info.get('label', 'N/A')
            values_dict = depth_info.get('values', {})
            value = values_dict.get('mean')
            
            if value is None:
                continue

            display_unit = unit
            if prop_name in ["clay", "sand", "silt"]:
                value /= 10
                display_unit = '%'
            elif prop_name == "phh2o":
                value /= 10
                display_unit = 'pH'
            
            processed_data.append({
                'property': prop_name,
                'depth': depth_label,
                'value': round(value, 2),
                'unit': display_unit
            })

    if not processed_data:
        print("UYARI: API isteği başarılı oldu ancak bu koordinatlar için bir toprak verisi bulunamadı.")
        print("Olası Neden: Belirtilen nokta kentsel bir alanda, su yüzeyinde veya SoilGrids'in veri sunmadığı bir bölgede olabilir.")
        return None

    df = pd.DataFrame(processed_data)
    return df

if __name__ == "__main__":
    # Hollanda, Flevoland'daki tarım arazisi koordinatları
    target_lat = 49.218299
    target_lon = 0.510686

    soil_df = get_soil_properties_for_point(lon=target_lon, lat=target_lat)

    if soil_df is not None and not soil_df.empty:
        print("Veri Çekme İşlemi Tamamlandı!")
        print("İşte sonuç tablosu:")
        print(soil_df.to_string())
    elif soil_df is not None:
         print("Uyarı: Veri başarıyla çekildi ancak işlenecek bir layer bulunamadı. Tablo boş.")