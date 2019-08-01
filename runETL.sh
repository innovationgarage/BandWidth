#!/bin/bash
npzpath=$1
for f in "$npzpath"/*.npz; do
  echo $f
  #python ETL_cluster.py --dumpfile $f --outpath outdata/
  parallel -j+0 --eta python ETL_cluster.py ::: $f
#  parallel -S "$CLUSTER_NODES" --nonall 'python ETL_cluster.py ::: '$f
done
