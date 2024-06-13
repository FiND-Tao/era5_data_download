import xarray as xr
import matplotlib.pyplot as plt
from tqdm import tqdm
import glob
import multiprocessing

outputfolder="/data/taoliu/taoliufile/era5/data/To_Jared_variable_year_dailymean"
files=glob.glob("/data/taoliu/taoliufile/era5/data/To_Jared_variable_year/*.nc")

def process_file(file):
    ds = xr.open_dataset(file)
    # Calculate the daily mean
    ds_daily_mean = ds.resample(time='D').mean()
    outputfile=f"{outputfolder}/{file.split('/')[-1].replace('variable','variable_dailymean')}"
    ds_daily_mean.to_netcdf(outputfile)

# Use a Pool to run the process_file function in parallel
with multiprocessing.Pool(processes=20) as pool:
    for _ in tqdm(pool.imap_unordered(process_file, files), total=len(files)):
        pass