#%%
import numpy as np
import pandas as pd 
# Define file paths
years=list(range(1976,2023))
pointIDs=list(range(1,11))
for year in years:
    for PointID in pointIDs:
        t2m_file = "/data/taoliu/taoliufile/era5/data/To_Jared_variable_year_dailymean_csv/PointID_{}_t2m_year_{}_daily_mean.csv".format(PointID,year)
        d2m_file = "/data/taoliu/taoliufile/era5/data/To_Jared_variable_year_dailymean_csv/PointID_{}_d2m_year_{}_daily_mean.csv".format(PointID,year)

        # Load the temperature and dew point temperature datasets
        t2m_ds = pd.read_csv(t2m_file)['t2m']
        d2m_ds = pd.read_csv(d2m_file)['d2m']

        #%%
        # Convert temperature values from Kelvin to Celsius
        t2m_celsius = t2m_ds - 273.15
        d2m_celsius = d2m_ds - 273.15

        # Calculate saturation vapor pressure (es) using the Clausius–Clapeyron equation
        def saturation_vapor_pressure(t):
            return 6.112 * np.exp((17.67 * t) / (t + 243.5))

        es = saturation_vapor_pressure(t2m_celsius)

        # Calculate actual vapor pressure (ea) using the dew point temperature
        ea = saturation_vapor_pressure(d2m_celsius)

        # Calculate relative humidity (RH)
        RH = (ea / es) * 100
        date=pd.read_csv(d2m_file)['date']
        # Create a new dataset containing only the relative humidity variable
        rh_ds = pd.DataFrame({'date':date,'rh': RH})
        outputname=d2m_file.replace('d2m','rh')
        rh_ds.to_csv(outputname, index=False)
# %%
