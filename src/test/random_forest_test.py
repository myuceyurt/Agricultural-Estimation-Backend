import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
import numpy as np

FILE_PATH = 'data/processed/final_training_data_with_soil.csv'

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

rf_model = RandomForestRegressor(n_estimators=100, random_state=42)

print("🧠 Model eğitiliyor...")
rf_model.fit(X_train, y_train)
print("✅ Model başarıyla eğitildi.\n")

print("⚙️  Daha önce görülmemiş test verileriyle tahmin yapılıyor...")
y_pred = rf_model.predict(X_test)

r2 = r2_score(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))

print("--- MODEL PERFORMANS SONUÇLARI ---")
print(f"R-Kare (R²) Skoru: {r2:.4f}")
print(f"Kök Ortalama Kare Hata (RMSE): {rmse:.4f}\n")

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

print("\n--- TEST SETİ TAHMİN DETAYLARI ---")
test_sonuclari = pd.DataFrame({'Gerçek Verim': y_test, 'Tahmin Edilen Verim': y_pred})
test_sonuclari['Fark'] = test_sonuclari['Gerçek Verim'] - test_sonuclari['Tahmin Edilen Verim']
print(test_sonuclari.to_string())