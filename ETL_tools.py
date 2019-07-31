#!/usr/bin/env python
import glob, os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder     .master('local')     .appName('ETL')     .config('spark.executor.memory', '5gb')     .config("spark.cores.max", "6")     .getOrCreate()
sc = spark.sparkContext
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, BooleanType, StringType

def read_npz_to_csv(filename, LBW):
    data = np.load(filename+'.npz')['packets']
    if not os.path.exists(filename+'.csv'):
        print("Converting " + filename + " to CSV...")
        pd_df = pd.DataFrame(data=data)
        pd_df.drop(columns=['src', 'dst', 'src_port', 'dst_port'], inplace=True)
        #if filename.split('_')[1]:
        pd_df['lbw'] = LBW        
        pd_df.to_csv(filename+'.csv', index=False)
    else:
        print(filename+'.csv' + " already exists")
        
def read_csv_to_df(csvfile):
    df = spark.read.option("header", "true").csv(csvfile) 
    return df

def find_ack_for_seq(df):
    df.createOrReplaceTempView("df")
    df_joined = spark.sql("""
                select df1.stream, df1.timestamp, df1.sent, df1.seqnum, min(df2.lbw) as lbw, min(df2.timestamp) as acktimestamp 
                from df df1 inner join df df2 
                on df1.seqnum<df2.acknum 
                and df1.sent!=df2.sent
                and df1.stream=df2.stream
                group by df1.stream, df1.timestamp, df1.seqnum, df1.sent
                order by df1.stream
                """)
    return df_joined

def shift_windows(df, streamno):
    shift_window = Window.partitionBy().orderBy('timestamp')
    #calculate sequence length
    df.createOrReplaceTempView("df")
    df_shifted = spark.sql("select * from df where stream={} and sent=True".format(streamno))
    df_shifted = df_shifted.withColumn('seqnum_1', F.lag(df_shifted.seqnum).over(shift_window))
    df_shifted = df_shifted.withColumn('seqlength', F.when(F.isnull(df_shifted.seqnum - df_shifted.seqnum_1), 0)
                          .otherwise(df_shifted.seqnum - df_shifted.seqnum_1))
    #calculate g_in
    df_shifted = df_shifted.withColumn('timestamp_1', F.lag(df_shifted.timestamp).over(shift_window))
    df_shifted = df_shifted.withColumn('gin', F.when(F.isnull(df_shifted.timestamp - df_shifted.timestamp_1), 0)
                          .otherwise(df_shifted.timestamp - df_shifted.timestamp_1))
    #calculate g_ack
    df_shifted = df_shifted.withColumn('acktimestamp_1', F.lag(df_shifted.acktimestamp).over(shift_window))
    df_shifted = df_shifted.withColumn('gack', F.when(F.isnull(df_shifted.acktimestamp - df_shifted.acktimestamp_1), 0)
                          .otherwise(df_shifted.acktimestamp - df_shifted.acktimestamp_1))
    # df_shifted.createOrReplaceTempView("df_shifted")
    # df_shifted = spark.sql("select * from df_shifted where df_shifted.seqlength>0") 
    return df_shifted

def get_modes(df):
    return df.seqlength.mode().values[0], df.gin.mode().values[0], df.gack.mode().values[0]

def get_medians(df):
    return df.seqlength.median(), df.gin.median(), df.gack.median()

def sanity_check(df):
    df.createOrReplaceTempView("df_shifted")
    df_sanity = spark.sql("""
                            select  timestamp, timestamp_1, seqnum, seqnum_1, seqlength, gin 
                            from df_shifted 
                            where df_shifted.seqlength<0 
                            and sent=True 
                            and stream=0 
                            order by timestamp asc
                            """)
    if df_sanity.count()==0:
        return True
    else:
        print(df_sanity.show())
        return False

def get_stream_arrays(df, streamno):
    df.createOrReplaceTempView("df_stream")
    stream = spark.sql("""
                        select * from df_stream
                        where df_stream.stream={}
                        """.format(streamno))\
    .rdd.map(lambda row: (row.timestamp, row.seqlength, row.gin, row.gack))
    ts_array = np.array(stream.map(lambda ts_l: float(ts_l[0])).collect())
    l_array = np.array(stream.map(lambda ts_l: float(ts_l[1])).collect())
    gin_array = np.array(stream.map(lambda ts_l: float(ts_l[2])).collect())
    gack_array = np.array(stream.map(lambda ts_l: float(ts_l[3])).collect())
    return ts_array, l_array, gin_array, gack_array
