import numpy as np
import pandas as pd
import os

# def merge_seq_ack(dfin, dfout):
#     for seqnum in df['seqnum'].unique():
#         #Logic needs double- & triple-checking
#         row = {}
#         row['seqnum'] = seqnum        
#         seqrows = dfin.loc[dfin.seqnum==seqnum]
#         row['seqdir'] = seqrows['sent'].values
#         row['seqtime'] = seqrows['timestamp'].values
#         row['seqsize'] = seqrows['l'].values   
#         ackrows = dfin.loc[(dfin.sent!=row['seqdir'])&(dfin.acknum>row['seqnum'])]
#         row['acknum'] = ackrows['acknum'].min()
#         row['acktime'] = ackrows['timestamp'].max()        
#         dfout = dfout.append(row, ignore_index=True)
#     dfout['seqtime_1'] = dfout['seqtime'].shift(periods=1)
#     dfout['acktime_1'] = dfout['acktime'].shift(periods=1)
#     dfout['gin'] = dfout['seqtime'] - dfout['seqtime_1']
#     dfout['gack'] = dfout['acktime'] - dfout['acktime_1']
#     return dfout

def merge_seq_ack(dfin, stream, dfout):
    for acknum in dfin['acknum'].unique():
        #Logic needs double- & triple-checking
        row = {}
        row['stream'] = stream
        row['acknum'] = acknum        
        ackrow = dfin.loc[(dfin.acknum==acknum)&(dfin.ack==True)].tail(1)
        row['ackdir'] = ackrow['sent'].values[0]
        row['acktime'] = ackrow['timestamp'].values[0]
        seqrow = dfin.loc[(dfin.sent!=row['ackdir'])&(dfin.seqnum<row['acknum'])].tail(1)
        if seqrow.shape[0]>0:
            row['seqnum'] = seqrow['seqnum'].values[0]
            row['seqtime'] = seqrow['timestamp'].values[0]
            row['seqsize'] = seqrow['l'].values[0]
            dfout = dfout.append(row, ignore_index=True)
    return dfout

#filename = '/home/saghar/Dualog/projects/BandWidth/mangledpgaps/7/8/9/6/9/9/9/3/7896,9,993/dumpfile.npz'
filename = 'dumpfile17.npz'
data = np.load(filename)['packets']
df = pd.DataFrame(data=data)
df.drop(columns=['src', 'dst', 'src_port', 'dst_port'], inplace=True)

#calculate package size
df['seqnum_1'] = df.groupby(['stream', 'sent'])['seqnum'].shift(periods=1)
df['l'] = df['seqnum'] - df['seqnum_1']

columns = ['seqnum', 'seqdir', 'seqtime', 'seqsize', 'acknum', 'acktime', 'stream']
flow = pd.DataFrame(columns=columns)
for stream in df.stream.unique():
    print(stream)
    print('------')
    dfin = df.loc[(df.stream==stream)&(df.ack==True)]
    flow = flow.append(merge_seq_ack(dfin, stream, flow))
    print(flow.shape)

f = flow.drop_duplicates().reset_index(drop=True)

f['seqtime_1'] = f['seqtime'].shift(periods=1)
f['acktime_1'] = f['acktime'].shift(periods=1)
f['gin'] = (f['seqtime'] - f['seqtime_1']).astype(float)
f['gack'] = (f['acktime'] - f['acktime_1']).astype(float)
