import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_error
import xgboost as xgb # XGBoost kütüphanesini ekledik

# 1. Veriyi Yükle
# Dosya yolunun doğru olduğundan emin ol (ai-service klasör yapısına göre)
FILE_PATH = 'ai-service/data/processed/final_training_data_with_soil(1).csv'
# Eğer direkt ana dizindeysen: 'data/processed/final_training_data_with_soil(1).csv'

try:
    df = pd.read_csv(FILE_PATH)
except FileNotFoundError:
    # Alternatif yol (Test ederken kolaylık olsun)
    df = pd.read_csv('data/processed/final_training_data_with_soil.csv')

# 2. FİLTRELEME: Sadece verilerin tam olduğu Sentinel-2 dönemini (2018+) alıyoruz
# %72 Başarının sırrı burası!
df_clean_period = df[df['yil'] >= 2018].copy()

print(f"Orijinal Veri Sayısı: {len(df)}")
print(f"Filtrelenmiş (2018-2024) Veri Sayısı: {len(df_clean_period)}")

# 3. Hedef Değişken Kontrolü
df_clean_period = df_clean_period.dropna(subset=['verim_ton_hektar'])

# 4. X ve y Ayrımı
features_to_drop = ['verim_ton_hektar', 'nnokta_id']
X = df_clean_period.drop(columns=features_to_drop, errors='ignore')
y = df_clean_period['verim_ton_hektar']

# Eksik veri kalmışsa (ki 2018 sonrasında çok az olmalı) doldur
X = X.fillna(X.mean())

# 5. Train/Test Split
# Veri azaldığı için test boyutunu biraz küçültüyorum (%15) ki eğitim için veri kalsın
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.15, random_state=42)

# --- MODELLER ---

# A. Random Forest
rf_model = RandomForestRegressor(n_estimators=200, random_state=42)
rf_model.fit(X_train, y_train)
rf_pred = rf_model.predict(X_test)
rf_r2 = r2_score(y_test, rf_pred)
rf_mae = mean_absolute_error(y_test, rf_pred)

# B. Sklearn Gradient Boosting (Klasik)
gb_model = GradientBoostingRegressor(n_estimators=200, random_state=42)
gb_model.fit(X_train, y_train)
gb_pred = gb_model.predict(X_test)
gb_r2 = r2_score(y_test, gb_pred)
gb_mae = mean_absolute_error(y_test, gb_pred)

# C. XGBoost (Extreme Gradient Boosting) - YENİ
xgb_model = xgb.XGBRegressor(
    n_estimators=300,       # Biraz daha fazla ağaç
    learning_rate=0.03,     # Daha hassas öğrenme
    max_depth=5,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    n_jobs=-1
)
xgb_model.fit(X_train, y_train)
xgb_pred = xgb_model.predict(X_test)
xgb_r2 = r2_score(y_test, xgb_pred)
xgb_mae = mean_absolute_error(y_test, xgb_pred)

# 8. Özellik Önem Düzeyleri (Hepsi Bir Arada)
importance_df = pd.DataFrame({
    'Feature': X.columns,
    'Random Forest': rf_model.feature_importances_,
    'Gradient Boosting': gb_model.feature_importances_,
    'XGBoost': xgb_model.feature_importances_
})

# Görselleştirme (3 Model Yan Yana)
plt.figure(figsize=(20, 6))

# RF Plot
plt.subplot(1, 3, 1)
imp_rf = importance_df.sort_values(by='Random Forest', ascending=False).head(15)
sns.barplot(x='Random Forest', y='Feature', data=imp_rf, palette='viridis')
plt.title(f'Random Forest\nR²: {rf_r2:.3f} | MAE: {rf_mae:.3f}')

# GB Plot
plt.subplot(1, 3, 2)
imp_gb = importance_df.sort_values(by='Gradient Boosting', ascending=False).head(15)
sns.barplot(x='Gradient Boosting', y='Feature', data=imp_gb, palette='magma')
plt.title(f'Gradient Boosting\nR²: {gb_r2:.3f} | MAE: {gb_mae:.3f}')

# XGB Plot 
plt.subplot(1, 3, 3)
imp_xgb = importance_df.sort_values(by='XGBoost', ascending=False).head(15)
sns.barplot(x='XGBoost', y='Feature', data=imp_xgb, palette='rocket')
plt.title(f'XGBoost (Kazanan)\nR²: {xgb_r2:.3f} | MAE: {xgb_mae:.3f}')

plt.tight_layout()
plt.savefig('model_comparison_results.png')

print("\n" + "="*50)
print("--- FİLTRELENMİŞ DÖNEM (2018-2024) SONUÇLARI ---")
print("="*50)
print(f"Random Forest       -> R²: {rf_r2:.4f} (MAE: {rf_mae:.3f})")
print(f"Gradient Boosting   -> R²: {gb_r2:.4f} (MAE: {gb_mae:.3f})")
print(f"XGBoost (Önerilen)  -> R²: {xgb_r2:.4f} (MAE: {xgb_mae:.3f})")
print("="*50)

print("\nEn Önemli 10 Özellik (XGBoost):")
print(imp_xgb[['Feature', 'XGBoost']].head(10).to_string(index=False))