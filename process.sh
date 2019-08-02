#! /bin/bash

cd /ymslanda/bandwidthestimator/BandWidth
source ../PassiveBandwidthEstimator/env/bin/activate

python3 ETL_cluster.py "$1"
