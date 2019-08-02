#!/bin/bash
#get list of directories with dumpfiles
for i in {1..8}
do
    echo $i
    find /ymslanda/bandwidthestimator/data/mangleddataorwhatever/"$i"/ -name '*.npz' -printf "%h\n" > tmp"$i"
    sort tmp"$i" | uniq > lst"$i"    
done
