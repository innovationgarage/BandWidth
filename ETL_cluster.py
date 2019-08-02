#!/usr/bin/env python
"""
USAGE: 
  python ETL_cluster.py --dumpfile test/dumpfile_10000_2_7500.npz --outpath outdata/
"""
import os
import numpy as np
import pandas as pd
import ETL_utils as tools
import argparse
from datetime import datetime

def process(dumpfile, outfile, tempdir):
    with open(outfile, 'w') as f:
        base = os.path.basename(dumpfile)
        filename = os.path.splitext(base)[0]
        LBW = base.split('_')[1]
        NFlow = base.split('_')[2]
        FlowBW = base.split('_')[3]
        f.write('filename,LBW,stream,l_mode,gin_mode,gack_mode,l_median,gin_median,gack_median')
        f.write('\n')
        t0 = datetime.now()
        df = tools.read_npz(dumpfile)
        t1 = datetime.now()
#        print('tools.read_npz ', (t1-t0).total_seconds())
        df_joined = tools.find_ack_for_seq(df)
        t2 = datetime.now()
#        print('tools.find_ack_for_seq', (t2-t1).total_seconds())
        for streamno in df.stream.unique():
#            print('STREAM', streamno)
            tin = datetime.now()
            df_shifted = tools.shift_windows(df_joined[df_joined.stream==streamno])
            tout = datetime.now()
#            print('tools.shift_windows', (tout-tin).total_seconds())
            df_shifted.to_csv(os.path.join(tempdir, 'shifted_{}.csv'.format(filename)), index=False)
            l_mode, gin_mode, gack_mode = tools.get_modes(df_shifted)
            l_median, gin_median, gack_median = tools.get_medians(df_shifted)
            f.write('{},{},{},{},{},{},{},{},{}'.format(filename,LBW,streamno,l_mode,gin_mode,gack_mode,l_median,gin_median,gack_median))
            f.write('\n')
    tfinal = datetime.now()
    print('Writing {} took {} seconds'.format(outfile, (tfinal-tin).total_seconds()))
    f.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process TCP simulations.')
    parser.add_argument('dumpfile', type=str, help='path to the dumpfile.npz to process')
    # parser.add_argument('outpath', type=str, default='outdata/', help='dir path for the processed csv file')
    # parser.add_argument('tempdir', type=str, default='temps', help='dir path for temp files')
    args = parser.parse_args()
    
    #outfile = os.path.join(args.outpath, os.path.splitext(os.path.basename(args.dumpfile))[0]+'.csv')
    base = os.path.splitext(os.path.basename(args.dumpfile))[0]
    dirname = os.path.dirname(args.dumpfile)

    tempdir = 'temps/'
    outpath = 'outdata/'
    outfile = os.path.join(outpath, base+'_aggs.csv')
    process(args.dumpfile, outfile, tempdir)
