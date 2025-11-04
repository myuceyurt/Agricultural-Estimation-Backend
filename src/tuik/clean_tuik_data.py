import pandas as pd
import re
import sys
from pathlib import Path

def clean_tuik_data():
    try:
        SCRIPT_DIR = Path(__file__).resolve().parent
        PROJECT_ROOT = SCRIPT_DIR.parent.parent
        
        # Orijinal dosya adı (Excel) ve yolu
        input_path = PROJECT_ROOT / 'data' / 'raw' / 'konya_tarim_raw.xls'
        output_path = PROJECT_ROOT / 'data' / 'processed' / 'konya_bugday_verim.csv'
        
        print(f"Input path: {input_path}")
        print(f"Output path: {output_path}")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        rows_to_skip = [0, 2, 4, 5]
        header_rows = [0, 1] 

        df = pd.read_excel(
            input_path,
            sheet_name='Sheet0',
            header=header_rows,
            skiprows=rows_to_skip,
            index_col=0
        )
        df.index.name = 'Yil'

        df.columns.names = ['Ilce_Raw', 'Metrik']
        df_stacked = df.stack(level='Ilce_Raw', future_stack=True) 
        df_processed = df_stacked.reset_index()

        ekilen_alan_col = 'Ekilen Alan ve 01.11.12.00.00. (Buğday, Durum Buğdayı Hariç) - Dekar'
        uretim_col = 'Üretim Miktarı ve 01.11.12.00.00. (Buğday, Durum Buğdayı Hariç) - Ton'

        if ekilen_alan_col not in df_processed.columns or uretim_col not in df_processed.columns:
            print("HATA: Beklenen 'Ekilen Alan' veya 'Üretim Miktarı' sütunları bulunamadı.")
            print("Lütfen Excel dosyasındaki başlıkları kontrol edin.")
            sys.exit(1)

        df_processed = df_processed.rename(columns={
            ekilen_alan_col: 'Ekilen_Alan_Dekar',
            uretim_col: 'Uretim_Ton'
        })

        def extract_ilce(raw_name):
            match = re.search(r'Konya\((.*?)\)', str(raw_name))
            return match.group(1) if match else raw_name

        df_processed['Ilce'] = df_processed['Ilce_Raw'].apply(extract_ilce)
        df_processed['Ekilen_Alan_Dekar'] = pd.to_numeric(df_processed['Ekilen_Alan_Dekar'], errors='coerce')
        df_processed['Uretim_Ton'] = pd.to_numeric(df_processed['Uretim_Ton'], errors='coerce')
        df_processed['Verim_Ton_Hektar'] = (df_processed['Uretim_Ton'] * 10) / df_processed['Ekilen_Alan_Dekar']

        final_df = df_processed[[
            'Yil',
            'Ilce',
            'Ekilen_Alan_Dekar',
            'Uretim_Ton',
            'Verim_Ton_Hektar'
        ]]

        final_df.to_csv(output_path, index=False, float_format='%.10f')

        print(f"Clean complete. {output_path.relative_to(PROJECT_ROOT)}")

    except FileNotFoundError:
        print(f"HATA: Dosya bulunamadı: {input_path}")
        sys.exit(1)
    except ImportError:
        print("HATA: 'pd.read_excel' için 'openpyxl' veya 'xlrd' kütüphanesi gerekli.")
        print("Lütfen 'pip install openpyxl' veya 'pip install xlrd' komutuyla kurun.")
        sys.exit(1)
    except KeyError as e:
        print(f"HATA: Beklenen bir sütun adı bulunamadı: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Beklenmeyen bir hata oluştu: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    clean_tuik_data()
