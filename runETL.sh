#!/bin/bash
#get list of directories with dumpfiles
# find ../ -name '*.npz' -printf "%h\n" > lst_all
# sort lst_all | uniq > lst

for i in 8
do
    echo $i
    find ./data/mangleddataorwhatever/"$i"/ -name '*.npz' -printf "%h\n" > lst"$i"
    sort lst"$i" | uniq > lst"$i"
    while IFS= read -r line
    do
	echo $line
	basedir=`basename "$line"`
	for f in "$basedir"
	do
	    parallel -S "$CLUSTER_NODES" --nonall 'python ETL_cluster.py ::: ' "$basedir"/$f
	done
    done < lst"$i"
done

