import ee
import base_gee

def get_image_thumbnail_url(lon, lat, date_start='2024-05-01', date_end='2024-09-30'):

    base_gee.init()

    point_of_interest = ee.Geometry.Point(lon, lat)

    image = ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED') \
        .filterBounds(point_of_interest) \
        .filterDate(date_start, date_end) \
        .sort('CLOUDY_PIXEL_PERCENTAGE') \
        .first()


    vis_params = {
        'bands': ['B4', 'B3', 'B2'],
        'min': 0,
        'max': 3000,
        'gamma': 1.4
    }

    thumbnail_url = image.getThumbURL({
        **vis_params,
        'region': point_of_interest.buffer(1500).bounds(),
        'dimensions': '800x800'
    })

    print("\nURL Başarıyla Oluşturuldu!\n")
    print(thumbnail_url)

if __name__ == "__main__":
    target_lon = 28.889618
    target_lat = 41.025764
    
    get_image_thumbnail_url(lon=target_lon, lat=target_lat)