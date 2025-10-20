import pandas as pd
import os
from tqdm import tqdm
from tuik.clean_tuik_data import clean_tuik_csv
from solidgrids.get_soil_properties_for_point import get_soil_properties_for_point
from gee.collect_point_data import collect_point_data

ILCE_KOORDINATLARI = {
    'Cihanbeyli': {'enlem': 38.6587, 'boylam': 32.9254},
    'Kulu': {'enlem': 39.0934, 'boylam': 33.0831},
    'Altınekin': {'enlem': 38.3189, 'boylam': 32.8465},
    'Kadınhanı': {'enlem': 38.2386, 'boylam': 32.2150},
}

PROCESSED_DATA_DIR = os.path.join('data', 'processed')
VERIM_FILE_PATH = os.path.join(PROCESSED_DATA_DIR, 'konya_bugday_verim.csv')
FINAL_TRAINING_DATA_PATH = os.path.join(PROCESSED_DATA_DIR, 'final_training_data.csv')

def pivot_soil_data(soil_df):
    """
    SoilGrids'ten gelen uzun (long) DataFrame'i, model için uygun olan
    geniş (wide) formata çevirir. (Örn: 'pH_0-5cm', 'kil_0-5cm' sütunları oluşturur)
    """
    if soil_df is None or soil_df.empty:
        return pd.DataFrame()

    soil_df['feature_name'] = soil_df['property'] + '_' + soil_df['depth'].str.replace('-', '_')
    
    pivoted = soil_df.pivot_table(index=['property'], columns='feature_name', values='value').reset_index(drop=True)
    
    pivoted.columns.name = None

    return pivoted.iloc[0].to_dict()


def main():

    if not os.path.exists(VERIM_FILE_PATH):
        print(f"'{VERIM_FILE_PATH}' bulunamadı. `clean_tuik_csv` çalıştırılıyor...")
        raw_path = os.path.join('data', 'raw', 'konya_tarim_raw.csv')
        clean_tuik_csv(raw_file_path=raw_path, processed_file_path=VERIM_FILE_PATH)
    
    try:
        verim_df = pd.read_csv(VERIM_FILE_PATH)
        print("✅ Verim verileri başarıyla okundu.")
        print(verim_df.head())
    except FileNotFoundError:
        print(f"❌ HATA: Verim dosyası '{VERIM_FILE_PATH}' bulunamadı. Lütfen önce ilk script'i çalıştırın.")
        return
    
    soil_data_cache = {}
    
    all_rows = []

    for index, row in tqdm(verim_df.iterrows(), total=verim_df.shape[0], desc="İlçeler İşleniyor"):
        ilce = row['Ilce']
        yil = row['Yil']
        
        if ilce not in ILCE_KOORDINATLARI:
            print(f"⚠️ UYARI: '{ilce}' için koordinat bulunamadı. Bu satır atlanıyor.")
            continue
            
        coords = ILCE_KOORDINATLARI[ilce]
        lat, lon = coords['enlem'], coords['boylam']

        if ilce not in soil_data_cache:
            print(f"\n🌱 '{ilce}' için toprak verisi çekiliyor (Bu işlem ilçe başına bir kez yapılır)...")
            raw_soil_df = get_soil_properties_for_point(lon, lat)
            pivoted_soil_data = pivot_soil_data(raw_soil_df)
            soil_data_cache[ilce] = pivoted_soil_data
        
        soil_features = soil_data_cache[ilce]

        print(f"🛰️ '{ilce}' için {yil} yılı GEE verileri çekiliyor...")
        date_start = f"{yil}-01-01"
        date_end = f"{yil}-08-31" 
        
        try:
            daily_data_df = collect_point_data(lon, lat, date_start, date_end)
        except Exception as e:
            print(f"❌ HATA: {ilce}-{yil} için GEE verisi çekilemedi: {e}. Bu satır atlanıyor.")
            continue

        if daily_data_df.empty:
            print(f"⚠️ UYARI: {ilce}-{yil} için GEE verisi boş geldi. Bu satır atlanıyor.")
            continue
            
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
            **soil_features,
            **gee_features,
            'verim_ton_hektar': row['Verim_Ton_Hektar']
        }
        all_rows.append(final_row)

    if not all_rows:
        print("❌ HATA: Hiçbir veri işlenemedi. Lütfen ayarları ve koordinatları kontrol edin.")
        return

    final_df = pd.DataFrame(all_rows)
    
    desired_order = [
        'nnokta_id', 'yil', 'enlem', 'boylam', 
    ]
    final_columns = desired_order + [col for col in final_df.columns if col not in desired_order]
    final_df = final_df[final_columns]
    
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    final_df.to_csv(FINAL_TRAINING_DATA_PATH, index=False, encoding='utf-8-sig')

    print("\n🎉🎉🎉 İşlem Tamamlandı! 🎉🎉🎉")
    print(f"Nihai eğitim verisi '{FINAL_TRAINING_DATA_PATH}' dosyasına kaydedildi.")
    print("\n--- Veri Seti Önizlemesi ---")
    print(final_df.to_string())


if __name__ == "__main__":
    main()