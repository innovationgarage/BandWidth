#!/bin/bash
input=$1
outdir=$2
while IFS= read -r line
do
    basedir=`basename "$line"`
    for dumpfile in "$line"/dumpfile*; do
	basename=`basename "$dumpfile"`
	newname=$(echo "$basename"_"$basedir".npz | tr , _)
	echo NN:$newname
	python pcaptotcpgaps.py "$dumpfile" "$outdir$newname" "$line"/interfaces
    done
done < "$input"
    
