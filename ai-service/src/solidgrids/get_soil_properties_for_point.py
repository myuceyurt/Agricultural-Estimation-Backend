import time
import requests
import pandas as pd

checkForSinglePoint = False

ILCE_KOORDINATLARI = {
    'Ahırlı': {'enlem': 37.4688, 'boylam': 32.1755},
    'Akören': {'enlem': 37.6650, 'boylam': 32.5180},
    'Akşehir': {'enlem': 38.396900, 'boylam': 31.395134},
    'Altınekin': {'enlem': 38.4010, 'boylam': 33.0400},
    'Beyşehir': {'enlem': 37.648755, 'boylam': 31.739495},
    'Bozkır': {'enlem': 37.1950, 'boylam': 32.2280},
    'Çeltik': {'enlem': 39.0080, 'boylam': 31.7950},
    'Cihanbeyli': {'enlem': 38.6800, 'boylam': 32.8600},
    'Çumra': {'enlem': 37.578325, 'boylam': 32.824190},
    'Derbent': {'enlem': 38.0790, 'boylam': 32.0500},
    'Derebucak': {'enlem': 37.4250, 'boylam': 31.6700},
    'Doğanhisar': {'enlem': 38.1410, 'boylam': 31.6900},
    'Emirgazi': {'enlem': 38.0410, 'boylam': 33.8250},
    'Ereğli': {'enlem': 37.4800, 'boylam': 34.0500},
    'Güneysınır': {'enlem': 37.2850, 'boylam': 32.7000},
    'Hadim': {'enlem': 36.9890, 'boylam': 32.4350},
    'Halkapınar': {'enlem': 37.3820, 'boylam': 34.1950},
    'Hüyük': {'enlem': 37.9420, 'boylam': 31.6200},
    'Ilgın': {'enlem': 38.300719, 'boylam': 31.872345},
    'Kadınhanı': {'enlem': 38.3000, 'boylam': 32.2800},
    'Karapınar': {'enlem': 37.7300, 'boylam': 33.5200},
    'Karatay': {'enlem': 37.9500, 'boylam': 32.6500},
    'Kulu': {'enlem': 39.1000, 'boylam': 33.0400},
    'Meram': {'enlem': 37.8200, 'boylam': 32.3800},
    'Sarayönü': {'enlem': 38.2800, 'boylam': 32.4100},
    'Selçuklu': {'enlem': 38.0000, 'boylam': 32.5000},
    'Seydişehir': {'enlem': 37.4350, 'boylam': 31.8700},
    'Taşkent': {'enlem': 36.9400, 'boylam': 32.4850},
    'Tuzlukçu': {'enlem': 38.4750, 'boylam': 31.6700},
    'Yalıhüyük': {'enlem': 37.3100, 'boylam': 32.0900},
    'Yunak': {'enlem': 38.8000, 'boylam': 31.7600},
}

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
    target_lat = 37.578325
    target_lon = 32.824190

    if checkForSinglePoint:
        soil_df = get_soil_properties_for_point(lon=target_lon, lat=target_lat)
        if soil_df is not None and not soil_df.empty:
            print("Veri çekme işlemi tamamlandı!")
            print("İşte sonuç tablosu:")
            print(soil_df.to_string())
        elif soil_df is not None:
            print("Uyarı: Veri başarıyla çekildi ancak işlenecek bir layer bulunamadı. Tablo boş.")
    else:
        for ilce in ILCE_KOORDINATLARI:
            soil_df = get_soil_properties_for_point(lon=ILCE_KOORDINATLARI[ilce]["boylam"], lat=ILCE_KOORDINATLARI[ilce]["enlem"])
            if soil_df is not None and not soil_df.empty:
                print(f"{ilce} için veri çekme işlemi tamamlandı!")
                print("İşte sonuç tablosu:")
                print(soil_df.to_string())
            elif soil_df is not None:
                print(f"{ilce} için uyarı: Veri başarıyla çekildi ancak işlenecek bir layer bulunamadı. Tablo boş.")
            time.sleep(13)