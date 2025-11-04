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


def process_row(row):

    ilce = row['Ilce']
    yil = row['Yil']
    
    if ilce not in ILCE_KOORDINATLARI:
        print(f"UYARI: '{ilce}' için koordinat bulunamadı. Bu satır atlanıyor.")
        return None
        
    coords = ILCE_KOORDINATLARI[ilce]
    lat, lon = coords['enlem'], coords['boylam']
    
    date_start = f"{yil}-03-01"
    date_end = f"{yil}-08-31" # Büyüme sezonuna odaklan
    
    try:
        daily_data_df = collect_point_data(lon, lat, date_start, date_end)
    except Exception as e:
        print(f"HATA: {ilce}-{yil} için GEE verisi çekilemedi: {e}. Bu satır atlanıyor.")
        return None

    if daily_data_df is None or daily_data_df.empty:
        print(f"UYARI: {ilce}-{yil} için GEE verisi boş geldi. Bu satır atlanıyor.")
        return None
        
    gee_features = {
        'toplam_yagis_mm': daily_data_df['precip_mm'].sum(),
        'maks_ndvi': daily_data_df['NDVI'].max(),
        'ort_ndvi': daily_data_df['NDVI'].mean(),
        'ort_temp_c': daily_data_df['temp_C'].mean()
    }
    
    final_row = {
        'nnokta_id': ilce,
        'yil': yil,
        'enlem': lat,
        'boylam': lon,
        **gee_features,
        'verim_ton_hektar': row['Verim_Ton_Hektar']
    }
    return final_row


def get_soil_properties_for_point(lon, lat):

    print(f"SoilGrids Analiz: Enlem={lat}, Boylam={lon}")

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
    except requests.exceptions.HTTPError as http_err:
        print(f"SoilGrids HTTP Hatası ({lat},{lon}): {http_err}\n   - Sunucu Cevabı: {response.text}")
        return None
    except requests.exceptions.RequestException as req_err:
        print(f"SoilGrids İstek Hatası ({lat},{lon}): {req_err}")
        return None
    except Exception as e:
        print(f"SoilGrids Genel Hata ({lat},{lon}): {e}")
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
            
            processed_data.append({
                'feature_name': feature_name,
                'value': round(value, 2)
            })

    if not processed_data:
        print(f"UYARI: {lat},{lon} için SoilGrids verisi bulunamadı/işlenemedi.")
        return None

    df = pd.DataFrame(processed_data)
    wide_df = df.set_index('feature_name').transpose()
    wide_df.reset_index(drop=True, inplace=True)
    
    return wide_df


def main():
    
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    training_df = None

    if not os.path.exists(FINAL_TRAINING_DATA_PATH):
        print(f"'{FINAL_TRAINING_DATA_PATH}' bulunamadı. GEE verileri çekilerek oluşturuluyor...")
        
        if not os.path.exists(VERIM_FILE_PATH):
            print(f"'{VERIM_FILE_PATH}' bulunamadı. `clean_tuik_data` çalıştırılıyor...")
            try:
                clean_tuik_data()
                print("`clean_tuik_data` tamamlandı.")
            except Exception as e:
                print(f"HATA: `clean_tuik_data` çalıştırılırken hata: {e}")
                return
    
        try:
            verim_df = pd.read_csv(VERIM_FILE_PATH)
            print(f"Verim verileri ('{VERIM_FILE_PATH}') başarıyla okundu.")
        except FileNotFoundError:
            print(f"HATA: Verim dosyası '{VERIM_FILE_PATH}' bulunamadı. `clean_tuik_data` çalışmasına rağmen dosya oluşmadı.")
            return
        except Exception as e:
            print(f"HATA: '{VERIM_FILE_PATH}' okunurken hata: {e}")
            return
       
        all_rows = []
        MAX_WORKERS = 31 # İlçe sayısı kadar worker

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(process_row, row) 
                for index, row in verim_df.iterrows()
            ]
            
            print(f"Tüm {len(futures)} GEE görev {MAX_WORKERS} iş parçacığı ile başlatıldı...")
            for future in tqdm(as_completed(futures), total=len(futures), desc="İlçeler İçin GEE Verisi İşleniyor"):
                try:
                    result = future.result()
                    if result: 
                        all_rows.append(result)
                except Exception as e:
                    print(f"Bir GEE iş parçacığında hata oluştu: {e}")

        if not all_rows:
            print("HATA: Hiçbir GEE verisi işlenemedi. Çıkılıyor.")
            return

        final_df = pd.DataFrame(all_rows)
        
        desired_order = ['nnokta_id', 'yil', 'enlem', 'boylam']
        final_columns = desired_order + [col for col in final_df.columns if col not in desired_order]
        final_df = final_df.reindex(columns=final_columns)
        
        final_df.to_csv(FINAL_TRAINING_DATA_PATH, index=False, encoding='utf-8-sig')
        print(f"\nGEE verileriyle '{FINAL_TRAINING_DATA_PATH}' dosyası oluşturuldu.")
        
        training_df = final_df.copy()

    else:
        print(f"'{FINAL_TRAINING_DATA_PATH}' zaten mevcut. Dosya okunuyor...")
        try:
            training_df = pd.read_csv(FINAL_TRAINING_DATA_PATH)
            print("Dosya başarıyla okundu.")
        except Exception as e:
            print(f"HATA: '{FINAL_TRAINING_DATA_PATH}' okunurken hata: {e}")
            return

    if training_df is None:
        print("HATA: GEE veri aşaması tamamlanamadı, 'training_df' boş. Çıkılıyor.")
        return

    print("\n--- SoilGrids Veri Ekleme Aşaması Başlatılıyor ---")

    unique_locations = training_df[['nnokta_id', 'enlem', 'boylam']].drop_duplicates().reset_index(drop=True)
    print(f"{len(unique_locations)} benzersiz ilçe konumu için toprak verisi çekilecek.")
    
    all_soil_data = []

    print("API hız limitine (5 istek/dakika) uymak için her ilçe arasında 13 saniye beklenecek.")

    for index, loc_row in tqdm(unique_locations.iterrows(), total=unique_locations.shape[0], desc="İlçeler İçin Toprak Verisi Çekiliyor"):
        ilce = loc_row['nnokta_id']
        lat = loc_row['enlem']
        lon = loc_row['boylam']
        
        try:
            soil_data_wide = get_soil_properties_for_point(lon, lat)
            
            if soil_data_wide is not None:
                soil_data_wide['nnokta_id'] = ilce
                all_soil_data.append(soil_data_wide)
            else:
                print(f"UYARI: {ilce} ({lat},{lon}) için toprak verisi alınamadı/bulunamadı. Bu ilçe için toprak sütunları boş (NaN) olacak.")
                
        except Exception as e:
            print(f"HATA: {ilce} için SoilGrids işlenirken kritik hata: {e}")
        
        if index < len(unique_locations) - 1:
            time.sleep(13)

    if not all_soil_data:
        print("UYARI: Hiçbir ilçe için toprak verisi çekilemedi. Dosya yine de oluşturulacak ancak toprak sütunları olmayacak.")
        final_df_with_soil = training_df.copy()
    else:
        final_soil_df = pd.concat(all_soil_data, ignore_index=True, sort=False)

        print("\nTüm toprak verileri çekildi. Ana veri seti ile birleştiriliyor...")
        
        final_df_with_soil = pd.merge(training_df, final_soil_df, on='nnokta_id', how='left')
    
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