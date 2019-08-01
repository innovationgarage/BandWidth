#!/usr/bin/env python
"""
USAGE: python ETL_multiple.py 
"""

import glob, os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder\
                    .master('local')\
                    .appName('ETL_test')\
                    .config('spark.executor.memory', '5gb')\
                    .config("spark.cores.max", "6")\
                    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, BooleanType, StringType
import ETL_tools as tools
from datetime import datetime

#Settings
LBWs = [2335, 10000]
filedir = 'test'
#streamno = 0
maxcount = 600

#df_comb = pd.DataFrame(columns=['filename', 'l', ])
with open('outdata/aggs.csv', 'w') as f:
    t0 = datetime.now()
    f.write('filename,LBW,stream,l_mode,gin_mode,gack_mode,l_median,gin_median,gack_median')
    f.write('\n')
    for LBW in LBWs:
        if not os.path.exists(os.path.join(filedir, 'shifted_{}.parquet'.format(LBW))):
            for filename in glob.glob(os.path.join(filedir, "dumpfile_{}_*.npz".format(LBW))):
                print(filename)
                filename = os.path.basename(filename).split('.npz')[0]
                tools.read_npz_to_csv(os.path.join(filedir,filename), LBW)
                df = tools.read_csv_to_df(os.path.join(filedir, "{}.csv".format(filename)))
                df_joined = tools.find_ack_for_seq(df)
                # plt.figure()
                for streamno in df.toPandas()['stream'].unique():
                    df_shifted = tools.shift_windows(df_joined, streamno)
                    df_shifted.toPandas().to_csv(os.path.join(filedir, 'shifted_{}.csv'.format(filename)), index=False)
                    #EXPERIMENT
                    #            df_temp = df_shifted.sample(False, 0.5, seed=42)
                    df_temp = df_shifted
                    df_temp.createOrReplaceTempView("df_temp")
                    df_valid = df_temp
                    pdfv = df_valid.toPandas()
                    l_mode, gin_mode, gack_mode = tools.get_modes(pdfv)
                    l_median, gin_median, gack_median = tools.get_medians(pdfv)
                    f.write('{},{},{},{},{},{},{},{},{}'.format(filename,LBW,streamno,l_mode,gin_mode,gack_mode,l_median,gin_median,gack_median))
                    f.write('\n')
                t1 = datetime.now()
                print(filename, ' took ', (t1-t0).total_seconds(), ' seconds')
                    # plt.plot(pdfv.seqlength/pdfv.gin, pdfv.gack/pdfv.gin, 'o', label=streamno)
                    # plt.plot(l_mode/gin_mode, gackn_mode/ginn_mode, '*k')
                    # plt.plot(l_median/gin_median, gack_median/gin_median, '*y')
                # plt.legend()
                # plt.ylim([-1,10])
                # plt.savefig('outplot/{}'.format(filename))
            
        # df_all = tools.read_csv_to_df(os.path.join(filedir, "shifted_dumpfile_{}_*.csv".format(LBW)))
        # df_all.write.parquet(os.path.join(filedir, 'shifted_{}.parquet'.format(LBW)))
#        plt.show()
    else:
        print('Loading the already mangled data...')
#        df_all = spark.read.parquet(os.path.join(filedir, 'shifted_{}.parquet'.format(LBW)))
t2 = datetime.now()
print('All processings took ', (t2-t0).total_seconds(), ' seconds')
f.close()
