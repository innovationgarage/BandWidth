#!/usr/bin/env python
"""
USAGE: 
  python ETL_cluster.py --dumpfile test/dumpfile_10000_2_7500.npz --outpath outdata/
"""
import os, sys
import numpy as np
import pandas as pd
import ETL_utils as tools
import argparse
from datetime import datetime

def process(dumpfile, outfile, tempdir):
    with open(outfile, 'w') as f:
        base = os.path.basename(os.path.dirname(dumpfile))
        base = base.replace(',', '_')
        filename = os.path.splitext(base)[0]

        print('BASE', base)
        LBW = int(base.split('_')[0]) #kbit/s
        print('LBW', LBW)
        NFlow = int(base.split('_')[1])
        FlowBW = int(base.split('_')[2]) #kbyte/s
#        FlowBW = FlowBW * 8 #kbit/s
        print('FlowBW', FlowBW)
        MainFlowBW = int(base.split('_')[3]) #kbyte/s
#        MainFlowBW = MainFlowBW * 8 #kbit/s
        print('MainFlowBW', MainFlowBW)
        # except:
        #     LBW = None
        #     NFlow = None
        #     FlowBW = None
        #     MainFlowBW = None
        f.write('filename,LBW,stream,l_mode,gin_mode,gack_mode,l_median,gin_median,gack_median,MainFlowBW')
        f.write('\n')
        t0 = datetime.now()
        df = tools.read_npz(dumpfile)
        t1 = datetime.now()
        df_joined = tools.find_ack_for_seq(df)
        print('DF_JOINED SIZE:', df_joined.shape)
        t2 = datetime.now()
        for streamno in df.stream.unique():
            tin = datetime.now()
            df_shifted = tools.shift_windows(df_joined[df_joined.stream==streamno]).reset_index(drop=True)
            tout = datetime.now()
            if df_shifted.shape[0]!=0:
                l_mode, gin_mode, gack_mode = tools.get_modes(df_shifted)
                l_median, gin_median, gack_median = tools.get_medians(df_shifted)
                f.write('{},{},{},{},{},{},{},{},{},{}'.format(filename,LBW,streamno,l_mode,gin_mode,gack_mode,l_median,gin_median,gack_median,MainFlowBW))
                f.write('\n')
            del df_shifted
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
    dumpfile = sys.argv[1]
    tempdir = 'temps/'
    outbase = 'outdata/'
    if '.npz' in dumpfile:
        base = os.path.basename(os.path.dirname(dumpfile))
        base = base.replace(',', '_')
        LBW = base.split('_')[0]
        MainFlowBW = base.split('_')[3]
        outpath = os.path.join(outbase, LBW)
        outfile = os.path.join(outpath, os.path.basename(dumpfile).split('.npz')[0]+'_'+base+'_aggs.csv')
        if ('.npz' in dumpfile):        
            if os.path.exists(outfile):
                print('{} already exists!'.format(outfile))
            else:
                process(dumpfile, outfile, tempdir)
        else:
            pass
