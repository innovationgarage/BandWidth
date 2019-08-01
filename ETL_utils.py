#!/usr/bin/env python
import glob, os
import numpy as np
import pandas as pd

def read_npz_to_csv(dumpfile):
    """
    Saves the .csv file with the same name at the same path as .npz
    """
    data = np.load(dumpfile)['packets']
    base = os.path.splitext(os.path.basename(dumpfile))[0]
    dirname = os.path.dirname(dumpfile)
    filepath = os.path.join(dirname, base+'.csv')
    if not os.path.exists(filepath):
        print("Converting " +  base + " to CSV...")
        pd_df = pd.DataFrame(data=data)
        pd_df.drop(columns=['src', 'dst', 'src_port', 'dst_port'], inplace=True)
        pd_df['linkbw'] = base.split('_')[1]
        pd_df['nflow'] = base.split('_')[2]
        pd_df['flowbw'] = base.split('_')[3]
        pd_df.to_csv(filepath, index=False)
    else:
        print(filepath + " already exists")

def read_npz(dumpfile):
    """
    Saves the .csv file with the same name at the same path as .npz
    """
    data = np.load(dumpfile)['packets']
    base = os.path.splitext(os.path.basename(dumpfile))[0]
    dirname = os.path.dirname(dumpfile)
    df = pd.DataFrame(data=data)
    df.drop(columns=['src', 'dst', 'src_port', 'dst_port'], inplace=True)
    df['linkbw'] = base.split('_')[1]
    df['nflow'] = base.split('_')[2]
    df['flowbw'] = base.split('_')[3]
    return df
        
def find_ack_for_seq(df):
    merged = df.merge(df, on='stream')
    conditioned = merged[(merged.seqnum_x<merged.acknum_y)&(merged.sent_x!=merged.sent_y)]
    grouped = conditioned.groupby(['stream', 'timestamp_x', 'seqnum_x', 'sent_x'])
    joined = conditioned.groupby(['stream', 'timestamp_x', 'seqnum_x', 'sent_x'])['timestamp_y'].min().reset_index()
    return joined#.rename(columns={'lbw_x':'lbw', 'nflow_x':'nflow', 'flowbw_x':'flowbw'})
    
def shift_windows(df):
    #FIXME! This needs double checks!
    df = df.sort_values(by='timestamp_x').reset_index(drop=True)
    df['seqlength'] = (df.seqnum_x.shift(periods=-1) - df.seqnum_x)
    df['gin'] = (df.timestamp_x.shift(periods=-1) - df.timestamp_x)
    df['gack'] = (df.timestamp_y.shift(periods=-1) - df.timestamp_y)

    df = df[(df.seqlength>0) & (df.gin>0) & (df.gack>0)]
    return df

# def shift_windows(df, streamno):
#     shift_window = Window.partitionBy().orderBy('timestamp')
#     #calculate sequence length
#     df.createOrReplaceTempView("df")
#     df_shifted = spark.sql("select * from df where stream={} and sent=True".format(streamno))
#     df_shifted = df_shifted.withColumn('seqnum_1', F.lag(df_shifted.seqnum).over(shift_window))
#     df_shifted = df_shifted.withColumn('seqlength', F.when(F.isnull(df_shifted.seqnum - df_shifted.seqnum_1), 0)
#                           .otherwise(df_shifted.seqnum - df_shifted.seqnum_1))
#     #calculate g_in
#     df_shifted = df_shifted.withColumn('timestamp_1', F.lag(df_shifted.timestamp).over(shift_window))
#     df_shifted = df_shifted.withColumn('gin', F.when(F.isnull(df_shifted.timestamp - df_shifted.timestamp_1), 0)
#                           .otherwise(df_shifted.timestamp - df_shifted.timestamp_1))
#     #calculate g_ack
#     df_shifted = df_shifted.withColumn('acktimestamp_1', F.lag(df_shifted.acktimestamp).over(shift_window))
#     df_shifted = df_shifted.withColumn('gack', F.when(F.isnull(df_shifted.acktimestamp - df_shifted.acktimestamp_1), 0)
#                           .otherwise(df_shifted.acktimestamp - df_shifted.acktimestamp_1))
#     # df_shifted.createOrReplaceTempView("df_shifted")
#     # df_shifted = spark.sql("select * from df_shifted where df_shifted.seqlength>0") 
#     return df_shifted

def get_modes(df):
    return df.seqlength.mode().values[0], df.gin.mode().values[0], df.gack.mode().values[0]

def get_medians(df):
    return df.seqlength.median(), df.gin.median(), df.gack.median()

# def sanity_check(df):
#     df.createOrReplaceTempView("df_shifted")
#     df_sanity = spark.sql("""
#                             select  timestamp, timestamp_1, seqnum, seqnum_1, seqlength, gin 
#                             from df_shifted 
#                             where df_shifted.seqlength<0 
#                             and sent=True 
#                             and stream=0 
#                             order by timestamp asc
#                             """)
#     if df_sanity.count()==0:
#         return True
#     else:
#         print(df_sanity.show())
#         return False

# def get_stream_arrays(df, streamno):
#     df.createOrReplaceTempView("df_stream")
#     stream = spark.sql("""
#                         select * from df_stream
#                         where df_stream.stream={}
#                         """.format(streamno))\
#     .rdd.map(lambda row: (row.timestamp, row.seqlength, row.gin, row.gack))
#     ts_array = np.array(stream.map(lambda ts_l: float(ts_l[0])).collect())
#     l_array = np.array(stream.map(lambda ts_l: float(ts_l[1])).collect())
#     gin_array = np.array(stream.map(lambda ts_l: float(ts_l[2])).collect())
#     gack_array = np.array(stream.map(lambda ts_l: float(ts_l[3])).collect())
#     return ts_array, l_array, gin_array, gack_array
