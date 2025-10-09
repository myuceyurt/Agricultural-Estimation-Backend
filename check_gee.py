import ee

try:
    # Earth Engine kütüphanesini başlatmayı dene
    ee.Initialize()
    print("✅ Başarılı! Google Earth Engine kimlik doğrulaması tamamlandı.")
    print("Python ortamın GEE sunucularına başarıyla bağlanabiliyor.")
except Exception as e:
    print("❌ Hata! Kimlik doğrulama başarısız oldu.")
    print(f"Detay: {e}")
    print("\nLütfen yukarıdaki adımları tekrar kontrol et.")