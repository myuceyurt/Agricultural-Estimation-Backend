import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Input

# Tensorflow'un daha az çıktı üretmesi için log seviyesini ayarlayalım
tf.get_logger().setLevel('ERROR')

# --- 1. VERİ YÜKLEME VE HAZIRLAMA (Orijinal kod ile aynı) ---
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


# --- 2. YENİ ADIM: VERİ ÖLÇEKLENDİRME (Sinir Ağları için KRİTİK) ---
# LSTM (ve genel olarak Sinir Ağları) en iyi performansı GİRDİ verileri
# (0-1 veya ortalama=0, std=1) arasına ölçeklendirildiğinde gösterir.
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

print("✅ Girdi verileri (X) sinir ağı için ölçeklendirildi (StandardScaler).\n")


# --- 3. YENİ ADIM: VERİYİ LSTM İÇİN YENİDEN ŞEKİLLENDİRME ---
# LSTM, 3 boyutlu veri bekler: (örnek_sayısı, zaman_adımı, özellik_sayısı)
# Bizim verimiz sıralı (time-series) olmadığı için, her bir satırı 
# 1 zaman adımlık (timestep=1) bir sekans olarak kabul edeceğiz.

# (örnek_sayısı, özellik_sayısı) -> (örnek_sayısı, 1, özellik_sayısı)
n_features = X_train_scaled.shape[1]
X_train_reshaped = X_train_scaled.reshape((X_train_scaled.shape[0], 1, n_features))
X_test_reshaped = X_test_scaled.reshape((X_test_scaled.shape[0], 1, n_features))

print(f"✅ Veri LSTM için yeniden şekillendirildi. Yeni boyut: {X_train_reshaped.shape}\n")


# --- 4. LSTM MODELİNİ OLUŞTURMA VE DERLEME ---

# Basit bir LSTM modeli tanımlıyoruz
lstm_model = Sequential([
    # Input katmanı, (1, n_features) şeklinde veri alacağını belirtir
    Input(shape=(1, n_features)),
    
    # 50 nöronlu bir LSTM katmanı. 
    # 'relu' aktivasyonu genellikle RNN'lerde iyi çalışır.
    LSTM(50, activation='relu'),
    
    # Çıkış katmanı: Regresyon yaptığımız için 1 nöronlu ve 
    # 'linear' (varsayılan) aktivasyonlu bir Dense katman.
    Dense(1) 
])

# Modeli derliyoruz. 
# Optimizatör: 'adam' iyi bir başlangıçtır.
# Kayıp (Loss) Fonksiyonu: 'mean_squared_error' (RMSE'nin karesi) regresyon için standarttır.
lstm_model.compile(optimizer='adam', loss='mean_squared_error')

print("--- MODEL MİMARİSİ ---")
lstm_model.summary()


# --- 5. MODELİ EĞİTME ---
print("\n🧠 LSTM Modeli eğitiliyor...")

# Modeli eğitiyoruz. 'epochs' tüm verinin üzerinden kaç kez geçileceğini,
# 'batch_size' ise her adımda kaç örneğin işleneceğini belirtir.
history = lstm_model.fit(
    X_train_reshaped, 
    y_train, 
    epochs=50,          # Epoch sayısını verinize göre artırıp azaltabilirsiniz
    batch_size=32,
    validation_data=(X_test_reshaped, y_test),
    verbose=0 # Eğitim sürecini sessize alır (0), görmek için 1 yapın
)

print("✅ Model başarıyla eğitildi.\n")


# --- 6. TAHMİN VE DEĞERLENDİRME ---
print("⚙️  Daha önce görülmemiş test verileriyle tahmin yapılıyor...")
# Tahmin yaparken ölçeklendirilmiş ve yeniden şekillendirilmiş X_test'i kullanırız
y_pred_scaled = lstm_model.predict(X_test_reshaped)

# Modelin çıktısı (n_samples, 1) şeklindedir, bunu (n_samples,) şekline getiririz
y_pred = y_pred_scaled.flatten()

# Metrikleri hesaplama (Orijinal kod ile aynı)
r2 = r2_score(y_test, y_pred)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))

print("--- MODEL PERFORMANS SONUÇLARI (LSTM) ---")
print(f"R-Kare (R²) Skoru: {r2:.4f}")
print(f"Kök Ortalama Kare Hata (RMSE): {rmse:.4f}\n")

# Yorumlama (Orijinal kod ile aynı)
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