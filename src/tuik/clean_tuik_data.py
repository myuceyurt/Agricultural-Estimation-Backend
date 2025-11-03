import pandas as pd
import re
import sys
from pathlib import Path

def clean_tuik_data():
    try:
        SCRIPT_DIR = Path(__file__).resolve().parent
        PROJECT_ROOT = SCRIPT_DIR.parent.parent
        input_path = PROJECT_ROOT / 'data' / 'raw' / 'konya_tarim_raw.xls'
        output_path = PROJECT_ROOT / 'data' / 'processed' / 'konya_bugday_verim.csv'
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

        uretim_cols = [col for col in df_processed.columns if 'Üretim Miktarı' in str(col)]
        if len(uretim_cols) == 0:
            sys.exit(1)

        uretim_col_name = uretim_cols[0]
        df_processed = df_processed.rename(columns={
            'Dekar': 'Ekilen_Alan_Dekar',
            uretim_col_name: 'Uretim_Ton'
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
        sys.exit(1)
    except Exception:
        sys.exit(1)

if __name__ == "__main__":
    clean_tuik_data()
