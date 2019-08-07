import pandas as pd
import os, sys
#inpath = sys.argv[1]
#outpath = sys.argv[2]
inpath = 'outdata/'
outfile = 'all_data'
all_files = []
# r=root, d=directories, f = files
for r, d, f in os.walk(inpath):
    for file in f:
        if '.csv' in file:
            all_files.append(os.path.join(r, file))
df = pd.DataFrame()
for i, filepath in enumerate(all_files):
    if i==0:
        tmp_df = pd.read_csv(filepath)
    else:
        try:
            tmp_df = pd.read_csv(filepath, header=0)
            df = df.append(tmp_df)
        except :
            print('{} is empty!'.format(filepath))
            
df['LBW'] = pd.to_numeric(df['LBW'])
df['nflow'] = df['filename'].apply(lambda x: int(x.split('_')[1]))
df['flowbw'] = df['filename'].apply(lambda x: int(x.split('_')[2]))
df['cross_trafficBW'] = (df['nflow']-1)*df['flowbw']
df['availableBW'] = df['LBW']-df['cross_trafficBW']
df['BW'] = df['nflow']*df['flowbw']
df['overflow'] = df['BW']>df['LBW']
df['l_over_gin'] = df['l_mode']/df['gin_mode']
df['gack_over_gin'] = df['gack_mode']/df['gin_mode']

df.to_csv('{}.csv'.format(outfile), index=False)
