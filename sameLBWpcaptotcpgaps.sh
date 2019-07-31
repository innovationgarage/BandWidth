#!/bin/bash
input=$1
outdir=$2
while IFS= read -r line
do
    echo "$line"
    basedir=`basename "$line"`
    rsync root@ymslanda-ext.innovationgarage.tech:/ymslanda/bandwidthestimator/data/"$line"/dumpfile "$outdir"dumpfile
    rsync root@ymslanda-ext.innovationgarage.tech:/ymslanda/bandwidthestimator/data/"$line"/interfaces "$outdir"interfaces
    newname=$(echo dumpfile_"$basedir".npz | tr , _)
    echo $newname
    python pcaptotcpgaps.py "$outdir"dumpfile "$outdir$newname" "$outdir"interfaces
done < "$input"
    
