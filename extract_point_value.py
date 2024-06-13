#%%
# author Tao Liu, MTU, June 13 2024
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
from tqdm import tqdm
import numpy as np
def find_nearest_valid(ds, lon, lat, var, radius=1):
    """Find the nearest valid value in a dataset for a given variable and coordinates."""
    # Select a subset of points within a certain radius
    subset = ds.sel(longitude=slice(lon-radius, lon+radius), latitude=slice(lat+radius,lat-radius))
    # Calculate the Euclidean distance to the points in the subset
    dist = np.sqrt((subset.longitude - lon)**2 + (subset.latitude - lat)**2)
    # Create a new dataset that only includes valid values
    valid_subset = subset.where(~np.isnan(subset[var]), drop=True)
    # Find the coordinates of the valid point with the smallest distance
    min_dist_coords = dist.where(dist == dist.where(~np.isnan(valid_subset[var])).min(), drop=True)
    nearest_lon = min_dist_coords.longitude.values.item()
    nearest_lat = min_dist_coords.latitude.values.item()
    # Select the nearest valid value
    nearest = valid_subset.sel(longitude=nearest_lon, latitude=nearest_lat)
    return nearest[var].to_dataframe().reset_index()

coordinates_csv = "/data/taoliu/taoliufile/era5/point_coordinates.csv"
years=list(range(1976,2023))
variables_list_longname = ['total_precipitation', '2m_temperature', '2m_dewpoint_temperature']
variables_list=['tp','t2m','d2m']
for idx,variable in enumerate(variables_list):
    variable_longname=variables_list_longname[idx]
    for year in tqdm(years):
        # Define file paths
        combined_file = "/data/taoliu/taoliufile/era5/data/To_Jared_variable_year_dailymean/ERA5-Land_daily_year_{}_variable_dailymean_{}.nc".format(year,variable_longname)

        # Load the combined NetCDF dataset
        combined_ds = xr.open_dataset(combined_file)
        #combined_ds = combined_ds.sortby(['latitude', 'longitude'])

        # Read the CSV file containing the coordinates
        coords_df = pd.read_csv(coordinates_csv)
        for rowindex,row in coords_df.iterrows():
            PointID=row['PointID']
            lat = row['Latitude']
            lon = row['Longitude']
            # Select the nearest grid point for the given latitude and longitude
            temp_series = combined_ds[variable].sel(longitude=lon, latitude=lat,method="nearest").to_dataframe().reset_index()           
            if temp_series[variable].isna().any():  # If temp_series contains any NaN values
                temp_series = find_nearest_valid(combined_ds, lon, lat,variable,radius=1)  # Use find_nearest_valid to find the nearest valid value

            # Rename the columns
            # Rename 'time' column to 'date'
            temp_series = temp_series.rename(columns={'time': 'date'})
            daily_mean_df = temp_series
            output_file = f"/data/taoliu/taoliufile/era5/data/To_Jared_variable_year_dailymean_csv/PointID_{PointID}_{variable}_year_{year}_daily_mean.csv".format(PointID,variable,year)
            # Save to CSV
            daily_mean_df.to_csv(output_file, index=False)

# %%
