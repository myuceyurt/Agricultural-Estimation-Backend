import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import numpy as np
import xgboost as xgb # XGBoost kütüphanesini içe aktarıyoruz

# --- 1. VERİ YÜKLEME VE HAZIRLAMA (Orijinal kod ile aynı) ---
FILE_PATH = 'data/processed/final_training_data.csv'

df = pd.read_csv(FILE_PATH)
print("✅ Veri başarıyla DataFrame olarak okundu.\n")

df = df.dropna(subset=['verim_ton_hektar'])
X = df.drop(columns=['nnokta_id', 'verim_ton_hektar'])
y = df['verim_ton_hektar']

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

print(f"Eğitim seti boyutu: {len(X_train)} satır")
print(f"Test seti boyutu: {len(X_test)} satır\n")

# --- 2. MODELİ OLUŞTURMA (XGBOOST) ---
# Not: XGBoost (ağaç tabanlı modeller) için veri ölçeklendirme (StandardScaler)
# genellikle GEREKLİ DEĞİLDİR. Bu adımı atlıyoruz.

# XGBoost Regresyon modelini tanımlıyoruz.
# Random Forest'taki gibi temel parametreleri belirleyelim.
# objective='reg:squarederror': Regresyon için standart kayıp fonksiyonu (RMSE'yi minimize eder).
xgb_model = xgb.XGBRegressor(
    n_estimators=100,       # Ağaç sayısı (RF'deki gibi)
    learning_rate=0.1,      # Öğrenme oranı (daha düşük değerler daha sağlam ama yavaş olabilir)
    max_depth=5,            # Her bir ağacın maksimum derinliği
    random_state=42,
    objective='reg:squarederror' 
)

# --- 3. MODELİ EĞİTME ---
print("🧠 XGBoost Modeli eğitiliyor...")
# Modeli, ölçeklendirilmemiş orijinal X_train ve y_train verileriyle eğitiyoruz
xgb_model.fit(X_train, y_train)
print("✅ Model başarıyla eğitildi.\n")


# --- 4. TAHMİN VE DEĞERLENDİRME ---
print("⚙️  Daha önce görülmemiş test verileriyle tahmin yapılıyor...")
# Tahmin yaparken orijinal X_test'i kullanırız
y_pred = xgb_model.predict(X_test)

# Metrikleri hesaplama (Orijinal kod ile aynı)
r2 = r2_score(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))

print("--- MODEL PERFORMANS SONUÇLARI (XGBOOST) ---")
print(f"R-Kare (R²) Skoru: {r2:.4f}")
print(f"Kök Ortalama Kare Hata (RMSE): {rmse:.4f}\n")


# --- 5. YORUMLAMA (Orijinal kod ile aynı) ---
print("--- YORUM ---")
if r2 < 0.5:
    print(f"Modelin R² skoru ({r2:.2f}) oldukça düşük. Bu, girdilerle verim arasında güçlü bir ilişki kuramadığını gösteriyor.")
    print(f"Tahminler, ortalama olarak gerçek değerden {rmse:.2f} ton/hektar kadar sapıyor.")
    print("Olası nedenler: Veri setindeki satır sayısının az olması veya girdilerin verimi açıklamak için yeterli olmaması.")
elif r2 < 0.75:
    print(f"Modelin R² skoru ({r2:.2f}) orta seviyede. Model, verimdeki değişkenliğin bir kısmını açıklamayı başarmış.")
    print(f"Tahminler, ortalama olarak gerçek değerden {rmse:.2f} ton/hektar kadar sapıyor.")
    print("Daha fazla veri ve daha çeşitli özellikler (toprak, fenoloji) ekleyerek performans artırılabilir.")
else:
    print(f"Modelin R² skoru ({r2:.2f}) gayet iyi! Model, verimdeki değişkenliğin önemli bir kısmını açıklamayı başarmış.")
    print(f"Tahminler, ortalama olarak gerçek değerden {rmse:.2f} ton/hektar kadar sapıyor.")
    print("Bu prototip, daha fazla veriyle çok daha güçlü bir modelin temelini oluşturabilir.")

# Tahmin detayları (Orijinal kod ile aynı)
print("\n--- TEST SETİ TAHMİN DETAYLARI ---")
test_sonuclari = pd.DataFrame({'Gerçek Verim': y_test, 'Tahmin Edilen Verim': y_pred})
test_sonuclari['Fark'] = test_sonuclari['Gerçek Verim'] - test_sonuclari['Tahmin Edilen Verim']
print(test_sonuclari.to_string())