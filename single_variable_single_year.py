import pandas as pd
import cdsapi
import os
from concurrent.futures import ThreadPoolExecutor

# Load point coordinates from CSV
point_file = "/data/taoliu/taoliufile/era5/point_coordinates.csv"
data = pd.read_csv(point_file)

# Calculate the extents
lat_min = data['Latitude'].min()-1
lat_max = data['Latitude'].max()+1
lon_min = data['Longitude'].min()-1
lon_max = data['Longitude'].max()+1
print(f"Latitude: {lat_min} to {lat_max}, Longitude: {lon_min} to {lon_max}")
# Define variables and parameters for data retrieval
folder_out = '/data/taoliu/taoliufile/era5/data/To_Jared_variable_year'
variables_list = ['total_precipitation', '2m_temperature', '2m_dewpoint_temperature']
downloaded_file = 'ERA5-Land_daily_year_{}_variable_{}.nc'
file_format = 'netcdf'
start_year = 1976
end_year = 2022
years = [str(year) for year in range(start_year, end_year + 1)]

c = cdsapi.Client()

def download_data(year, variable):
    downloaded_file_path = os.path.join(folder_out, downloaded_file.format(year, variable))
    
    c.retrieve(
        'reanalysis-era5-land',
        {
            'product_type': 'reanalysis',
            'format': file_format,
            'variable': variable,
            'year': year,
            'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
            'day': [str(day).zfill(2) for day in range(1, 32)],
            'time': [
                '00:00', '01:00', '02:00', '03:00', '04:00', '05:00',
                '06:00', '07:00', '08:00', '09:00', '10:00', '11:00',
                '12:00', '13:00', '14:00', '15:00', '16:00', '17:00',
                '18:00', '19:00', '20:00', '21:00', '22:00', '23:00',
            ],
            'area': [lat_max, lon_min, lat_min, lon_max],  # [N, W, S, E]
        },
        downloaded_file_path
    )
    print(f"Downloaded data for year {year}, variable {variable}")

# Create a ThreadPoolExecutor to download data in parallel
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = []
    for year in years:
        for variable in variables_list:
            downloaded_file_path = os.path.join(folder_out, downloaded_file.format(year, variable))
            if os.path.exists(downloaded_file_path):
                print(f"Data for year {year}, variable {variable} already exists. Skipping download.")
                continue            
            futures.append(executor.submit(download_data,year, variable))

    # Ensure all futures are completed
    for future in futures:
        future.result()

print("All downloads completed.")
