#!/bin/bash
#get list of directories with dumpfiles
# find ../ -name '*.npz' -printf "%h\n" > lst_all
# sort lst_all | uniq > lst

for i in {1..8}
do
    while IFS= read -r line
    do
	ls "$line"/*
	    
    done < lst"$i"
done |
  parallel -S "$CLUSTER_NODES" '/ymslanda/bandwidthestimator/BandWidth/process.sh {}'

