import pandas as pd
from datetime import datetime
import os
import numpy as np
import shutil

custom_date_parser = lambda x: datetime.strptime(x, "%Y.%m.%d_%H.%M.%S")

WINDOW_SIZE = '320s'
MIN_PERIODS = 320
JUMP_SIZE_S = '30'
FULL_TRACE_BITRATE_FILTER_MAX_AVG = 13000
FULL_TRACE_BITRATE_FILTER_MIN = 0

SUBTRACE_BITRATE_FILTER_MAX_AVG = 13000
SUBTRACE_BITRATE_FILTER_MIN = 0

DATA_PATH = "./pedestrian/"
OUTPUT_PATH = './pedestrian_cooked/'

files = os.listdir(DATA_PATH)

if not os.path.exists(OUTPUT_PATH):
    os.mkdir(OUTPUT_PATH)
else:
  shutil.rmtree(OUTPUT_PATH)
  os.mkdir(OUTPUT_PATH)

for f in files:
  file_path = DATA_PATH +  f
  output_path = OUTPUT_PATH + f
  output_path_name, output_extension = os.path.splitext(output_path)

  print(file_path)

  #read in the csv file into dataframe and drop duplicates
  df = pd.read_csv(file_path, index_col='Timestamp', parse_dates=['Timestamp'], date_parser=custom_date_parser)
  df = df[~df.index.duplicated(keep='first')]
  
  #df.info()
  df = df.fillna(method='pad').asfreq('s', method='pad')

  mean_dl_bitrate = df['DL_bitrate'].mean()
  min_dl_bitrate = df['DL_bitrate'].min()

  #if mean_dl_bitrate > FULL_TRACE_BITRATE_FILTER_MAX_AVG or min_dl_bitrate < FULL_TRACE_BITRATE_FILTER_MIN:
  #  print("Skipping because too high or low mean bitrate!!")
  #  continue

  jump_time = df.index.min()
  jump_counter = 0

  for rolling_subset in df.rolling(WINDOW_SIZE):
        
    if rolling_subset.index.min() >= jump_time and rolling_subset.index.size >= MIN_PERIODS:
      # if x second step then actually save the subset
      #print("Rolling Size + " + str(rolling_subset.count()))
      #rolling_subset.info()
      
      df_subset = pd.DataFrame(rolling_subset, columns=["DL_bitrate"])
      # df_subset = df_subset[::-1] 
      
      dl_s = df_subset['DL_bitrate']
      subset_mean_dl_bitrate = dl_s.mean()
      subset_min_dl_bitrate = dl_s.min()
      subset_fraction_zeros = dl_s[dl_s == 0].count()/dl_s.count()
      #subset_min_dl_bitrate < SUBTRACE_BITRATE_FILTER_MIN or
      
      if subset_mean_dl_bitrate > SUBTRACE_BITRATE_FILTER_MAX_AVG or subset_fraction_zeros > 0.10:
        print("Skipping because too high mean bitrate or too many disconnections (bitrate == 0)!!")
        continue
      
      df_subset['unixtime'] = (df_subset.index - pd.Timestamp('1970-01-01')) // pd.Timedelta('1s')
      df_subset['unixtime'] = df_subset['unixtime'] - df_subset['unixtime'].min()
      
      #print("output trace!!" + str(df_subset.index.min()))
      
      df_subset.to_csv(output_path_name + "_" + str(jump_counter) + output_extension, columns=['unixtime','DL_bitrate'], header=False, index=False, date_format='%s')
      
      jump_time += np.timedelta64(JUMP_SIZE_S, 's')
      jump_counter += 1

