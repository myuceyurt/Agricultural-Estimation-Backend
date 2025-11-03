import pandas as pd
import os
from tqdm import tqdm
from tuik.clean_tuik_data import clean_tuik_data
from gee.collect_point_data import collect_point_data
from pathlib import Path

ILCE_KOORDINATLARI = {
    'Cihanbeyli': {'enlem': 38.691886, 'boylam': 32.889980},
    'Altınekin': {'enlem': 38.402561, 'boylam': 33.003392},
    'Kadınhanı': {'enlem': 38.322982, 'boylam': 32.307863},
}

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DATA_DIR = PROJECT_ROOT / 'data' / 'processed'
VERIM_FILE_PATH = PROCESSED_DATA_DIR / 'konya_bugday_verim.csv'
FINAL_TRAINING_DATA_PATH = PROCESSED_DATA_DIR / 'final_training_data.csv'


def main():

    if not os.path.exists(VERIM_FILE_PATH):
        print(f"'{VERIM_FILE_PATH}' bulunamadı. `clean_tuik_data` çalıştırılıyor...")
        clean_tuik_data()
    
    try:
        verim_df = pd.read_csv(VERIM_FILE_PATH)
        print("Verim verileri başarıyla okundu.")
        print(verim_df.head())
    except FileNotFoundError:
        print(f"HATA: Verim dosyası '{VERIM_FILE_PATH}' bulunamadı. Lütfen önce ilk script'i çalıştırın.")
        return
       
    all_rows = []

    for index, row in tqdm(verim_df.iterrows(), total=verim_df.shape[0], desc="İlçeler İşleniyor"):
        ilce = row['Ilce']
        yil = row['Yil']
        
        if ilce not in ILCE_KOORDINATLARI:
            print(f"UYARI: '{ilce}' için koordinat bulunamadı. Bu satır atlanıyor.")
            continue
            
        coords = ILCE_KOORDINATLARI[ilce]
        lat, lon = coords['enlem'], coords['boylam']

        print(f"🛰️ '{ilce}' için {yil} yılı GEE verileri çekiliyor...")
        date_start = f"{yil}-01-01"
        date_end = f"{yil}-12-31"       
        try:
            daily_data_df = collect_point_data(lon, lat, date_start, date_end)
        except Exception as e:
            print(f"HATA: {ilce}-{yil} için GEE verisi çekilemedi: {e}. Bu satır atlanıyor.")
            continue

        if daily_data_df.empty:
            print(f"UYARI: {ilce}-{yil} için GEE verisi boş geldi. Bu satır atlanıyor.")
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
            **gee_features,
            'verim_ton_hektar': row['Verim_Ton_Hektar']
        }
        all_rows.append(final_row)

    if not all_rows:
        print("HATA: Hiçbir veri işlenemedi. Lütfen ayarları ve koordinatları kontrol edin.")
        return

    final_df = pd.DataFrame(all_rows)
    
    desired_order = [
        'nnokta_id', 'yil', 'enlem', 'boylam', 
    ]
    final_columns = desired_order + [col for col in final_df.columns if col not in desired_order]
    final_df = final_df[final_columns]
    
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    final_df.to_csv(FINAL_TRAINING_DATA_PATH, index=False, encoding='utf-8-sig')

    print("\nİşlem Tamamlandı!")
    print(f"Nihai eğitim verisi '{FINAL_TRAINING_DATA_PATH}' dosyasına kaydedildi.")
    print("\n--- Veri Seti Önizlemesi ---")
    print(final_df.to_string())


if __name__ == "__main__":
    main()