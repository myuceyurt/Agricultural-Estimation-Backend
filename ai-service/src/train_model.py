import pandas as pd
import xgboost as xgb
import joblib
import os

INPUT_FILE = 'ai-service/data/processed/final_training_data_with_soil(1).csv'
OUTPUT_MODEL = 'ai-service/data/processed/konya_bugday_modeli_xgb.joblib'

def train_and_save():
    print(f"📂 Veri yükleniyor: {INPUT_FILE}...")
    
    if not os.path.exists(INPUT_FILE):
        print("❌ HATA: Veri dosyası bulunamadı! Lütfen önce create_training_data.py'yi çalıştırın.")
        return

    df = pd.read_csv(INPUT_FILE)

    print(f"   - Toplam veri: {len(df)} satır")
    df = df[(df['yil'] >= 2018) & (df['yil'] <= 2024)]
    print(f"   - Filtrelenmiş (2018-2024) veri: {len(df)} satır")

    # 3. TEMİZLİK
    df = df.dropna(subset=['verim_ton_hektar']) # Hedef boşsa sil
    df = df.fillna(0) # Diğer boşlukları 0 yap

    # X (Özellikler) ve y (Hedef) ayrımı
    X = df.drop(columns=['verim_ton_hektar', 'nnokta_id']) # İlçe isimleri (string) atılır
    y = df['verim_ton_hektar']

    # 4. MODEL EĞİTİMİ (Final Parametreler)
    print("🚀 Model eğitiliyor (XGBoost)...")
    model = xgb.XGBRegressor(
        n_estimators=300,
        learning_rate=0.03,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
        n_jobs=-1
    )
    
    model.fit(X, y)
    print("✅ Eğitim tamamlandı.")

    # 5. MODELİ KAYDETME
    joblib.dump(model, OUTPUT_MODEL)
    print(f"💾 Model başarıyla kaydedildi: {OUTPUT_MODEL}")
    
    # Test amaçlı bir tahmin yapalım
    print("\n--- Test Tahmini (İlk Satır) ---")
    sample_input = X.iloc[[0]]
    prediction = model.predict(sample_input)[0]
    actual = y.iloc[0]
    print(f"Gerçek Verim: {actual}")
    print(f"Tahmin      : {prediction:.4f}")

if __name__ == "__main__":
    train_and_save()