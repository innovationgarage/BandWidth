import os
import numpy as np
from scipy import optimize
import pandas as pd
import matplotlib.pyplot as plt
df = pd.read_csv('all_data.csv')

def piecewise_linear(x, x0, y0, k1, k2):
    y0 = 1
    return np.piecewise(x, [x < x0, x >= x0], [y0, lambda x:k2*x + y0-k2*x0])

def fit_piecewise(df_lbw, outname):
    x = df_lbw['l_over_gin'].values
    y = df_lbw['gack_over_gin'].values    
    p , e = optimize.curve_fit(piecewise_linear, x, y)
    xd = np.linspace(0, df_lbw['l_over_gin'].max(), 100)
    print(df_lbw.LBW.unique()[0], p)
    plt.figure()
    plt.plot(x, y, "o")
    plt.plot(xd, piecewise_linear(xd, *p))
    LBW = df_lbw.LBW.unique()[0]
    cross_traffic = ((df_lbw.nflow.unique()[0]-1)*df_lbw.flowbw.unique()[0])
    plt.title('LinkBW:{}'. format(lbw))
    plt.savefig(os.path.join('plots', '{}.png'.format(outname)))
    
dfs = []
for lbw in df.LBW.unique():
    df_lbw = df[df.LBW==lbw]
    print(lbw, df_lbw.l_mode.nunique())
    dfs.append(df_lbw)
    fit_piecewise(df_lbw, lbw)
    

# fig, axs = plt.subplots(df.LBW.nunique(), 1, figsize=(15,15))
# axs = axs.flatten()
# for i, dflbw in enumerate(dfs):
#     axs[i].plot(dflbw.l_over_gin,
#                 dflbw.gack_over_gin,
#                 '.',
#                 label=dflbw.LBW.unique()[0])
#     axs[i].legend(loc='upper right')
#     axs[i].set_ylim(0, 10)
#     if i==df.LBW.nunique()//2:
#         axs[i].set_ylabel('gack_mode/gin_mode')
# axs[i].set_xlabel('l_mode/gin_mode')
# plt.savefig('all_data.png')

#plt.show()


