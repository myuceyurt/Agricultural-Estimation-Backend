import pandas as pd
import numpy as np
import os

def clean_tuik_csv(raw_file_path, processed_file_path):
    try:
        with open(raw_file_path, 'r', encoding='utf-8-sig') as f:
            lines = f.readlines()
            if len(lines) < 7:
                print("❌ HATA: CSV dosyası beklenen formatta değil. En az 7 satır olmalı.")
                return
            header_line = lines[1]
        
        ilceler = [name.strip() for name in header_line.split(',') if 'Konya' in name]
        ilceler = [ilce.split('(')[1].split(')')[0] for ilce in ilceler]
        
        if len(ilceler) != 4:
            print(f"❌ HATA: Beklenen 4 ilçe yerine {len(ilceler)} ilçe bulundu. CSV başlık satırını kontrol edin.")
            return

        print(f"✅ İlçeler başarıyla bulundu: {ilceler}")
        
        df = pd.read_csv(
            raw_file_path,
            header=None,
            skiprows=6
        )
        print("✅ Ham CSV veri kısmı başarıyla okundu.")

    except FileNotFoundError:
        print(f"❌ HATA: Dosya bulunamadı! '{raw_file_path}' yolunu kontrol edin.")
        return
    except Exception as e:
        print(f"❌ HATA: CSV dosyası okunurken bir sorun oluştu: {e}")
        return

    
    df = df.iloc[:, :9]
    
    new_columns = ['Yil']
    for ilce in ilceler:
        new_columns.append(f"{ilce}_EkilenAlan_da")
        new_columns.append(f"{ilce}_Uretim_ton")
    
    df.columns = new_columns
    print("✅ Sütun başlıkları atandı.")

    df_melted = pd.melt(df, id_vars=['Yil'], var_name='Metric', value_name='Deger')
    df_melted.dropna(subset=['Deger'], inplace=True)
    df_melted[['Ilce', 'Gosterge']] = df_melted['Metric'].str.split('_', n=1, expand=True)
    df_melted = df_melted.drop(columns=['Metric'])
    print("✅ Veri 'uzun' formata çevrildi.")

    df_final = df_melted.pivot_table(index=['Yil', 'Ilce'], columns='Gosterge', values='Deger', aggfunc='first').reset_index()
    df_final.columns.name = None

    print("\n--- PIVOT SONRASI GERÇEK SÜTUN ADLARI ---")
    print(list(df_final.columns))

    df_final.rename(columns={
            'EkilenAlan_da': 'Ekilen_Alan_Dekar',
            'Uretim_ton': 'Uretim_Ton'
        }, inplace=True)    
    for col in ['Yil', 'Ekilen_Alan_Dekar', 'Uretim_Ton']:
        df_final[col] = pd.to_numeric(df_final[col], errors='coerce')
    df_final.dropna(inplace=True)
    df_final['Yil'] = df_final['Yil'].astype(int)

    df_final['Verim_Ton_Hektar'] = np.where(
        df_final['Ekilen_Alan_Dekar'] > 0,
        df_final['Uretim_Ton'] / (df_final['Ekilen_Alan_Dekar'] / 10),
        0
    )
    df_final = df_final.sort_values(by=['Ilce', 'Yil']).reset_index(drop=True)
    print("✅ Nihai tablo oluşturuldu ve verim hesaplandı.")

    # --- 5. Kaydetme ---
    output_dir = os.path.dirname(processed_file_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        
    df_final.to_csv(processed_file_path, index=False, encoding='utf-8-sig')
    print(f"\n🎉 Veriler başarıyla '{processed_file_path}' dosyasına kaydedildi!")
    
    print("\n--- ÖNİZLEME ---")
    print(df_final.to_string())


if __name__ == "__main__":
    script_path = os.path.abspath(__file__)
    src_dir = os.path.dirname(script_path)
    project_root = os.path.dirname(src_dir)
    raw_path = os.path.join(project_root, 'data', 'raw', 'konya_tarim_raw.csv')
    processed_path = os.path.join(project_root, 'data', 'processed', 'konya_bugday_verim.csv')
    
    clean_tuik_csv(raw_file_path=raw_path, processed_file_path=processed_path)