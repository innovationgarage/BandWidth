#!/bin/bash
CLUSTER_NODES=n2,n3,n4,n5,n6,n7,n8,n9,n10,n11,n12,n13,n14,n15,n16,n17,n18,n19,n20,n21,n22,n23,n24,n25,n26,n27,n28,n29,n30,n31,n32,n33,n34,n35,n36,n37,n38,n39,n40
for i in 1
do
    echo $i
    while IFS= read -r line
    do
	ls "$line"/*
	echo $line
    done < lst"$i"
done |
  parallel -S "$CLUSTER_NODES" '/ymslanda/bandwidthestimator/BandWidth/process.sh {}'

