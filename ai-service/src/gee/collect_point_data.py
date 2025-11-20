import os
import ee
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count
import time

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterator, **kwargs): return iterator

try:
    import base_gee 
except ImportError:
    try:
        from . import base_gee
    except ImportError:
        print("UYARI: base_gee bulunamadı. GEE init işlemi bu dosyada manuel yapılacak.")
        class base_gee:
            @staticmethod
            def init():
                try:
                    ee.Initialize()
                except:
                    ee.Authenticate()
                    ee.Initialize()

def get_monthly_means(image_collection, roi, start_date, end_date, band_name, reducer=None, scale=30):
    if reducer is None:
        reducer = ee.Reducer.mean()

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    monthly_data = {}
    current = start

    while current < end:
        next_month = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
        if next_month > end:
            next_month = end

        date_str_start = current.strftime("%Y-%m-%d")
        date_str_end = next_month.strftime("%Y-%m-%d")
        
        month_map = {
            1:'Jan', 2:'Feb', 3:'Mar', 4:'Apr', 5:'May', 6:'Jun',
            7:'Jul', 8:'Aug', 9:'Sep', 10:'Oct', 11:'Nov', 12:'Dec'
        }
        month_abbr = month_map[current.month]

        monthly_col = image_collection.filterDate(date_str_start, date_str_end)
        img_reduced = monthly_col.mean()
        
        try:
            result_dict = img_reduced.reduceRegion(
                reducer=reducer,
                geometry=roi,
                scale=scale,
                maxPixels=1e9,
                bestEffort=True,
                tileScale=4
            ).getInfo()
            
            if result_dict and band_name in result_dict:
                val_local = result_dict[band_name]
            else:
                val_local = None
        except Exception:
            val_local = None

        monthly_data[f"{band_name}_{month_abbr}"] = val_local
        current = next_month
        
    return monthly_data

def get_elevation(roi):
    try:
        srtm = ee.Image("USGS/SRTMGL1_003")
        result = srtm.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=roi,
            scale=100,
            maxPixels=1e9,
            bestEffort=True
        ).getInfo()
        return result.get('elevation')
    except:
        return None

def preprocess_landsat(image):
    qa = image.select('QA_PIXEL')
    mask = (qa.bitwiseAnd(1 << 3).eq(0).And(qa.bitwiseAnd(1 << 4).eq(0)))
    optical_bands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
    return image.addBands(optical_bands, overwrite=True).updateMask(mask)

def collect_point_data(lon, lat, date_start='2020-03-01', date_end='2020-08-31', region_radius=3000):
    try:
        base_gee.init()
    except Exception:
        return None

    point_geom = ee.Geometry.Point([lon, lat])
    roi = point_geom.buffer(region_radius) 
    
    start_year = datetime.strptime(date_start, "%Y-%m-%d").year

    if start_year >= 2016:
        ndvi_col = (
            ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
            .filterBounds(roi)
            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 50))
            .map(lambda img: img.addBands(img.normalizedDifference(['B8', 'B4']).rename('NDVI')))
            .select('NDVI')
        )
        scale_ndvi = 20

    elif start_year >= 2013:
        ndvi_col = (
            ee.ImageCollection("LANDSAT/LC08/C02/T1_L2")
            .filterBounds(roi)
            .map(preprocess_landsat)
            .map(lambda img: img.addBands(img.normalizedDifference(['SR_B5', 'SR_B4']).rename('NDVI')))
            .select('NDVI')
        )
        scale_ndvi = 30

    elif start_year == 2012:
        ndvi_col = (
            ee.ImageCollection("LANDSAT/LE07/C02/T1_L2")
            .filterBounds(roi)
            .map(preprocess_landsat)
            .map(lambda img: img.addBands(img.normalizedDifference(['SR_B4', 'SR_B3']).rename('NDVI')))
            .select('NDVI')
        )
        scale_ndvi = 30

    else:
        ndvi_col = (
            ee.ImageCollection("LANDSAT/LT05/C02/T1_L2")
            .filterBounds(roi)
            .map(preprocess_landsat)
            .map(lambda img: img.addBands(img.normalizedDifference(['SR_B4', 'SR_B3']).rename('NDVI')))
            .select('NDVI')
        )
        scale_ndvi = 30

    rain_col = ee.ImageCollection('UCSB-CHG/CHIRPS/DAILY').filterBounds(roi).select('precipitation')

    era5_col = (
        ee.ImageCollection('ECMWF/ERA5_LAND/DAILY_AGGR')
        .filterBounds(roi)
        .map(lambda img: img.expression(
            '((MIN + MAX) / 2) - 273.15',
            {'MIN': img.select('temperature_2m_min'), 'MAX': img.select('temperature_2m_max')}
        ).rename('temp_C').copyProperties(img, ['system:time_start']))
        .select('temp_C')
    )

    try:
        ndvi_monthly = get_monthly_means(ndvi_col, roi, date_start, date_end, 'NDVI', scale=scale_ndvi)

        if all(v is None for v in ndvi_monthly.values()):
            ndvi_modis = (
                ee.ImageCollection('MODIS/061/MOD13Q1')
                .filterBounds(roi)
                .filterDate(date_start, date_end)
                .select('NDVI')
                .map(lambda img: img.multiply(0.0001).copyProperties(img, ['system:time_start']))
            )
            ndvi_monthly = get_monthly_means(ndvi_modis, roi, date_start, date_end, 'NDVI', scale=250)

        rain_monthly = get_monthly_means(rain_col, roi, date_start, date_end, 'precipitation', reducer=ee.Reducer.sum(), scale=5566)
        rain_monthly = {k.replace('precipitation', 'Rain'): v for k, v in rain_monthly.items()}

        temp_monthly = get_monthly_means(era5_col, roi, date_start, date_end, 'temp_C', scale=11132)
        elevation = get_elevation(roi)

        final_data = {
            'Latitude': lat,
            'Longitude': lon,
            'elevation': elevation,
            **ndvi_monthly,
            **rain_monthly,
            **temp_monthly
        }
        
        df_row = pd.DataFrame([final_data])
        
        for col in df_row.columns:
            df_row[col] = pd.to_numeric(df_row[col], errors='coerce')
            
        cols_to_interpolate = [c for c in df_row.columns if 'NDVI' in c or 'temp' in c]
        if cols_to_interpolate:
             df_row[cols_to_interpolate] = df_row[cols_to_interpolate].interpolate(method='linear', axis=1, limit_direction='both')
        
        df_row = df_row.fillna(0)
        
        return df_row
    
    except Exception:
        return None

def worker_task(args):
    lon, lat, start, end = args
    return collect_point_data(lon, lat, date_start=start, date_end=end)

if __name__ == "__main__":
    print("Veri seti hazırlık testi başlatılıyor...")

    list_of_points = [
        (32.50, 38.00),
        (33.10, 37.50),
    ]

    input_tasks = []
    for year in range(2006, 2025):
        start = f"{year}-03-01"
        end = f"{year}-08-31"
        for lon, lat in list_of_points:
            input_tasks.append((lon, lat, start, end))

    print(f"Toplam görev sayısı: {len(input_tasks)}")

    num_workers = min(10, cpu_count())
    print(f"{num_workers} worker kullanılıyor.")

    results = []
    
    with Pool(processes=num_workers) as pool:
        for result in tqdm(pool.imap(worker_task, input_tasks), total=len(input_tasks)):
            if result is not None:
                results.append(result)

    if results:
        final_df = pd.concat(results, ignore_index=True)
        final_df = final_df.dropna(axis=0, how='all')

        cols = final_df.columns.tolist()
        priority = ['Latitude', 'Longitude', 'elevation']
        others = [c for c in cols if c not in priority]
        final_cols = priority + others
        
        final_df = final_df[final_cols]

        print("\n--- İŞLEM BAŞARILI ---")
        print(f"Toplam Satır: {len(final_df)}")
        print("İlk 5 Satır:")
        print(final_df.head())
        
        filename = f"gee_results_fixed_{datetime.now().strftime('%Y%m%d')}.csv"
        final_df.to_csv(filename, index=False)
        print(f"Dosya kaydedildi: {filename}")
    else:
        print("HATA: Hiçbir veri çekilemedi.")
