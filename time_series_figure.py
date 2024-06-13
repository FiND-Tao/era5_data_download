#%%
# author Tao Liu, MTU, June 13 2024

import pandas as pd
import glob
from matplotlib.dates import YearLocator, DateFormatter
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
variables_list_longname = ['total_precipitation', '2m_temperature', '2m_dewpoint_temperature','relative_humidity']
variables_list=['tp','t2m','d2m','rh']
pointID_list=[1,2,3,4,5,6,7,8,9,10]
for idx,variable in enumerate(variables_list):
    variable_longname=variables_list_longname[idx]
    for PointID in pointID_list:
        files=glob.glob("/data/taoliu/taoliufile/era5/data/To_Jared_variable_year_dailymean_csv/PointID_{}_{}_*.csv".format(PointID,variable))
        files.sort()
        df_list = []
        for file in files:
            df = pd.read_csv(file)
            df_list.append(df)
        combined_df = pd.concat(df_list, ignore_index=True)
        if variable in ['t2m','d2m']:
            combined_df[variable] = combined_df[variable] - 273.15
        fig, ax = plt.subplots(figsize=(50, 6))

        # Convert 'date' column to datetime
        combined_df['date'] = pd.to_datetime(combined_df['date'])

        # Plot the data
        ax.plot(combined_df['date'], combined_df[variable], label='Data')


        # Set the locator
        locator = YearLocator()
        ax.xaxis.set_major_locator(locator)

        # Specify the format - %Y is for full year
        formatter = DateFormatter('%Y')
        ax.xaxis.set_major_formatter(formatter)

        # Increase the font size of the axis tick labels
        ax.tick_params(axis='both', labelsize=20)

        ax.set_xlabel('Date', fontsize=20)
        ax.set_ylabel(variable_longname, fontsize=20)
        ax.set_title('Time Series Plot for PointID {}'.format(PointID), fontsize=24)

        # Add a legend
        ax.legend()

        plt.show()

        # Save the figure
        fig.savefig('/data/taoliu/taoliufile/era5/data/figures/time_series_plot_PointID_{}_{}.png'.format(PointID,variable))
# %%
