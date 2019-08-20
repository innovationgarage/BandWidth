#!/bin/bash
#get list of directories with dumpfiles
for i in 1
do
    echo $i
    find /ymslanda/bandwidthestimator/data4/mangleddata/"$i"/ -name '*.npz' -printf "%h\n" > tmp"$i"
    sort tmp"$i" | uniq > lst"$i"    
done
