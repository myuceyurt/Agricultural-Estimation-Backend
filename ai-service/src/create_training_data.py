import pandas as pd
import os
from tqdm import tqdm
import time
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from tuik.clean_tuik_data import clean_tuik_data
from gee.collect_point_data import collect_point_data

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

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DATA_DIR = PROJECT_ROOT / 'data' / 'processed'
VERIM_FILE_PATH = PROCESSED_DATA_DIR / 'konya_bugday_verim.csv'
FINAL_TRAINING_DATA_PATH = PROCESSED_DATA_DIR / 'final_training_data.csv'
FINAL_TRAINING_DATA_WITH_SOIL_PATH = PROCESSED_DATA_DIR / 'final_training_data_with_soil.csv'

def get_soil_properties_for_point(lon, lat):
    BASE_URL = "https://rest.isric.org/soilgrids/v2.0/properties/query"
    params = {
        'lon': lon,
        'lat': lat,
        'property': ["clay", "sand", "silt", "phh2o", "cec", "soc"],
        'depth': ["0-5cm", "5-15cm", "15-30cm"],
        'value': ["mean"]
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception:
        print(f"Toprak verisi alınamadı: lon={lon}, lat={lat}")
        return None

    processed_data = []
    properties = data.get('properties', {})
    layers = properties.get('layers', [])

    for layer in layers:
        prop_name = layer.get('name', 'N/A')
        for depth_info in layer.get('depths', []):
            depth_label = depth_info.get('label', 'N/A')
            values_dict = depth_info.get('values', {})
            value = values_dict.get('mean')
            if value is None:
                continue

            if prop_name in ["clay", "sand", "silt"]:
                value /= 10
            elif prop_name == "phh2o":
                value /= 10
            elif prop_name == "soc":
                value /= 10

            feature_name = f"soil_{prop_name}_{depth_label.replace('-', '_')}"
            processed_data.append({'feature_name': feature_name, 'value': round(value, 2)})

    if not processed_data:
        return None

    df = pd.DataFrame(processed_data)
    wide_df = df.set_index('feature_name').transpose()
    wide_df.reset_index(drop=True, inplace=True)
    return wide_df

def process_row(row):
    ilce = row['Ilce']
    yil = row['Yil']
    
    if ilce not in ILCE_KOORDINATLARI:
        return None
        
    coords = ILCE_KOORDINATLARI[ilce]
    lat, lon = coords['enlem'], coords['boylam']
    
    date_start = f"{yil}-03-01"
    date_end = f"{yil}-08-31" 
    
    try:
        gee_df = collect_point_data(
            lon=lon,
            lat=lat,
            date_start=date_start,
            date_end=date_end,
            region_radius=5000
        )
    except Exception:
        return None

    if gee_df is None or gee_df.empty:
        return None
        
    gee_features = gee_df.iloc[0].to_dict()
    
    final_row = {
        'nnokta_id': ilce,
        'yil': yil,
        'enlem': lat,
        'boylam': lon,
        **gee_features,
        'verim_ton_hektar': row['Verim_Ton_Hektar']
    }
    return final_row

def main():
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    training_df = None
    
    if not os.path.exists(FINAL_TRAINING_DATA_PATH):
        print(f"'{FINAL_TRAINING_DATA_PATH}' oluşturuluyor...")
        
        if not os.path.exists(VERIM_FILE_PATH):
            try:
                clean_tuik_data()
            except Exception as e:
                print(f"TUIK verisi hazırlanırken hata: {e}")
                return
    
        try:
            verim_df = pd.read_csv(VERIM_FILE_PATH)
        except Exception as e:
            print(f"Verim dosyası okunamadı: {e}")
            return
       
        all_rows = []
        MAX_WORKERS = 12

        print(f"\n--- GEE Verileri İndiriliyor (Toplam {len(verim_df)} Satır) ---")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_row, row) for _, row in verim_df.iterrows()]
            
            for future in tqdm(as_completed(futures), total=len(futures), unit="işlem", desc="GEE İndirme"):
                result = future.result()
                if result: 
                    all_rows.append(result)

        if not all_rows:
            print("Hata: GEE verisi oluşturulamadı.")
            return

        final_df = pd.DataFrame(all_rows)
        final_df = final_df.dropna(axis=1, how='all')
        
        cols = ['nnokta_id', 'yil', 'enlem', 'boylam']
        remaining_cols = [c for c in final_df.columns if c not in cols and c != 'verim_ton_hektar']
        final_order = cols + remaining_cols + ['verim_ton_hektar']
        final_order = [c for c in final_order if c in final_df.columns]
        
        final_df = final_df[final_order]
        final_df.to_csv(FINAL_TRAINING_DATA_PATH, index=False, encoding='utf-8-sig')
        training_df = final_df.copy()
        print(f"GEE aşaması tamamlandı: {FINAL_TRAINING_DATA_PATH}")

    else:
        print(f"GEE verisi zaten var, okunuyor: {FINAL_TRAINING_DATA_PATH}")
        training_df = pd.read_csv(FINAL_TRAINING_DATA_PATH)

    if training_df is None or training_df.empty:
        return

    print("\n--- SoilGrids Veri Ekleme Aşaması ---")
    
    unique_locations = training_df[['nnokta_id', 'enlem', 'boylam']].drop_duplicates().reset_index(drop=True)
    all_soil_data = []

    for index, loc_row in tqdm(unique_locations.iterrows(), total=unique_locations.shape[0], desc="Toprak Verisi", unit="ilçe"):
        ilce = loc_row['nnokta_id']
        lat = loc_row['enlem']
        lon = loc_row['boylam']
        
        try:
            soil_data_wide = get_soil_properties_for_point(lon, lat)
            if soil_data_wide is not None:
                soil_data_wide['nnokta_id'] = ilce
                all_soil_data.append(soil_data_wide)
        except Exception:
            pass 
        
        if index < len(unique_locations) - 1:
            time.sleep(13)

    if all_soil_data:
        final_soil_df = pd.concat(all_soil_data, ignore_index=True, sort=False)
        final_df_with_soil = pd.merge(training_df, final_soil_df, on='nnokta_id', how='left')
    else:
        final_df_with_soil = training_df.copy()
    
    if 'verim_ton_hektar' in final_df_with_soil.columns:
        yield_col = final_df_with_soil.pop('verim_ton_hektar')
        final_df_with_soil['verim_ton_hektar'] = yield_col
    
    final_df_with_soil.to_csv(FINAL_TRAINING_DATA_WITH_SOIL_PATH, index=False, encoding='utf-8-sig')

    print("\nİşlem Tamamlandı!")
    print(f"Nihai eğitim verisi (toprak verileri dahil) '{FINAL_TRAINING_DATA_WITH_SOIL_PATH}' dosyasına kaydedildi.")
    print("\n--- Yeni Veri Seti Önizlemesi (İlk 10 Satır) ---")
    print(final_df_with_soil.head(10).to_string())
    print("\n--- Yeni Veri Seti Sütun Bilgisi ---")
    final_df_with_soil.info()

if __name__ == "__main__":
    main()
