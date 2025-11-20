import ee
import sys

def init():
    try:
        ee.Initialize(project='agro-estimation-project')
        print("GEE kimlik doğrulaması başarılı.")
    except Exception as e:
        print(f"GEE başlatılamadı. Hata: {e}", file=sys.stderr)
        sys.exit(1)