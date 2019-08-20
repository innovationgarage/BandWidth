#!/usr/bin/env python
import glob, os
import numpy as np
import pandas as pd

# def read_npz_to_csv(dumpfile):
#     """
#     Saves the .csv file with the same name at the same path as .npz
#     """
#     data = np.load(dumpfile)['packets']
#     base = os.path.splitext(os.path.basename(dumpfile))[0]
#     dirname = os.path.dirname(dumpfile)
#     filepath = os.path.join(dirname, base+'.csv')
#     if not os.path.exists(filepath):
#         print("Converting " +  base + " to CSV...")
#         pd_df = pd.DataFrame(data=data)
#         pd_df.drop(columns=['src', 'dst', 'src_port', 'dst_port'], inplace=True)
#         pd_df.to_csv(filepath, index=False)
#     else:
#         print(filepath + " already exists")

def read_npz(dumpfile):
    """
    Saves the .csv file with the same name at the same path as .npz
    """
    data = np.load(dumpfile)['packets']
    base = os.path.splitext(os.path.basename(dumpfile))[0]
    dirname = os.path.dirname(dumpfile)
    df = pd.DataFrame(data=data)
    df.drop(columns=['src', 'dst'], inplace=True)
    #timestamp is originally in microseconds but we want it in seconds in the end: Let's convert!
    df['timestamp'] = df['timestamp'] * 1e-6 #sec
    return df
        
def find_ack_for_seq(df):
    merged = df.merge(df, on='stream')
    print('MERGED SIZE:', merged.shape)
    if not os.path.exists('test_df.csv'):
        merged.to_csv('test_df.csv', index=False)
    merged = merged[((merged.src_port_x==1024)&(merged.sent_x==False))|((merged.dst_port_x==1024)&(merged.sent_x==True))]
    conditioned = merged[(merged.seqnum_x<merged.acknum_y)&(merged.sent_x!=merged.sent_y)]
    grouped = conditioned.groupby(['stream', 'timestamp_x', 'seqnum_x', 'sent_x'])
    joined = conditioned.groupby(['stream', 'timestamp_x', 'seqnum_x', 'sent_x'])['timestamp_y'].min().reset_index()
    return joined#.rename(columns={'lbw_x':'lbw', 'nflow_x':'nflow', 'flowbw_x':'flowbw'})
    
def shift_windows(df):
    #FIXME! This needs double checks!
    df = df.sort_values(by='timestamp_x').reset_index(drop=True)
    df['seqlength'] = (df.seqnum_x.shift(periods=-1) - df.seqnum_x) #byte
#    df['seqlength'] * df['seqlength'] * 8 #bit
    df['seqlength'] * df['seqlength'] / 1024. #kbyte
    df['gin'] = (df.timestamp_x.shift(periods=-1) - df.timestamp_x)
    df['gack'] = (df.timestamp_y.shift(periods=-1) - df.timestamp_y)

    df = df[(df.seqlength>0) & (df.gin>0) & (df.gack>0)]
    return df

def get_modes(df):
    return df.seqlength.mode().values[0], df.gin.mode().values[0], df.gack.mode().values[0]

def get_medians(df):
    return df.seqlength.median(), df.gin.median(), df.gack.median()
