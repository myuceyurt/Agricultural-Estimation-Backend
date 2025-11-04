import os
import ee
import pandas as pd
from datetime import datetime
from . import base_gee
# import base_gee

def collect_point_data(lon, lat, date_start='2006-05-08', date_end='2006-06-08', output_dir="data/processed"):

    base_gee.init()

    point = ee.Geometry.Point([lon, lat])

    # --- NDVI (SENTINEL 2 for >2015, MODIS for <2015 ) ---

    start_year = datetime.strptime(date_start, "%Y-%m-%d").year
    if start_year < 2015:
        ndvi_collection = (
            ee.ImageCollection('MODIS/061/MOD13Q1')
            .filterBounds(point)
            .filterDate(date_start, date_end)
            .select('NDVI')
            .map(lambda img: img.multiply(0.0001).copyProperties(img, ['system:time_start']))
        )
    else:
        ndvi_collection = (
            ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
            .filterBounds(point)
            .filterDate(date_start, date_end)
            .map(lambda img: img.addBands(
                img.normalizedDifference(['B8', 'B4']).rename('NDVI')
            ).copyProperties(img, ['system:time_start']))
            .select('NDVI')
        )

    ndvi_fc = ndvi_collection.map(lambda img: ee.Feature(None, {
        'date': ee.Date(img.get('system:time_start')).format('YYYY-MM-dd'),
        'NDVI': img.reduceRegion(ee.Reducer.mean(), point, 250).get('NDVI')
    }))

    print(f"NDVI görüntü sayısı: {ndvi_collection.size().getInfo()}")

    # --- Yağış (CHIRPS) ---
    chirps = (
        ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY')
        .filterBounds(point)
        .filterDate(date_start, date_end)
        .select('precipitation')
    )

    rain_fc = chirps.map(lambda img: ee.Feature(None, {
        'date': ee.Date(img.get('system:time_start')).format('YYYY-MM-dd'),
        'precip_mm': img.reduceRegion(ee.Reducer.mean(), point, 5000).get('precipitation')
    }))

    print(f"Yağış görüntü sayısı: {chirps.size().getInfo()}")

    # --- Günlük Sıcaklık (ERA5-Land DAILY_AGGR) ---
    era5 = (
        ee.ImageCollection('ECMWF/ERA5_LAND/DAILY_AGGR')
        .filterBounds(point)
        .filterDate(date_start, date_end)
        .map(lambda img: img.addBands(
            img.select(['temperature_2m_min', 'temperature_2m_max']).reduce(ee.Reducer.mean()).rename('temp_C')
        ))
        .select('temp_C')
    )

    temp_fc = era5.map(lambda img: ee.Feature(None, {
        'date': ee.Date(img.get('system:time_start')).format('YYYY-MM-dd'),
        'temp_C': ee.Number(img.reduceRegion(ee.Reducer.mean(), point, 10000).get('temp_C')).subtract(273.15)
    }))

    print(f"Günlük sıcaklık görüntü sayısı: {era5.size().getInfo()}")

    # --- EE FeatureCollection -> Pandas DataFrame ---
    def fc_to_df(fc, limit=5000):
            fc_size = fc.size().getInfo()
            print(f"Özellik koleksiyonu boyutu: {fc_size}")

            num_to_process = min(fc_size, limit)
            
            if num_to_process == 0:
                print("Uyarı: Koleksiyon boş. Boş DataFrame döndürülüyor.")
                return pd.DataFrame()

            features = fc.toList(num_to_process)
            
            data = []
            for i in range(num_to_process):
                try:
                    props = features.get(i).getInfo()['properties']
                    print(f"İşlenen özellik {i+1}/{num_to_process}: {props}")
                    data.append(props)
                except Exception as e:
                    print(f"Özellik {i} işlenirken HATA oluştu: {e}")
            
            return pd.DataFrame(data)

    ndvi_df = fc_to_df(ndvi_fc)
    rain_df = fc_to_df(rain_fc)
    temp_df = fc_to_df(temp_fc)

    print("\nNDVI DataFrame:")
    print(ndvi_df.head())
    print("\nYağış DataFrame:")
    print(rain_df.head())
    print("\nSıcaklık DataFrame:")
    print(temp_df.head())

    # --- DataFrame'leri birleştir ---
    merged = pd.merge(rain_df, ndvi_df, on='date', how='outer')
    merged = pd.merge(merged, temp_df, on='date', how='outer')

    merged = merged.sort_values('date').reset_index(drop=True)

    # --- NDVI ileri doldur ---
    merged['NDVI'] = merged['NDVI'].ffill()

    os.makedirs(output_dir, exist_ok=True)
    filename = f"point_timeseries_{lat:.4f}_{lon:.4f}.csv"

    full_path = os.path.join(output_dir, filename)
    merged.to_csv(full_path, index=False, encoding='utf-8-sig')

    print(f"\nVeriler başarıyla kaydedildi: {full_path}")
    print("\nMerged DataFrame (ilk 10 satır):")
    print(merged.head(10))

    return merged


if __name__ == "__main__":
    target_lon = 28.889618
    target_lat = 41.025764
    df = collect_point_data(lon=target_lon, lat=target_lat)
