#get list of directories with dumpfiles
#from BandWidth/
find /ymslanda/bandwidthestimator/data4/data -name '*interfaces*' -printf "%h\n" | sort -u > dumpfiles.lst

#convert dumpfiles into npz files using "interfaces"
#from PassiveBandwidthEstimator/
bash gridpcaptotcpgaps.sh --indir=/ymslanda/bandwidthestimator/data4/data --outdir=/ymslanda/bandwidthestimator/data4/mangleddata
#bash sameLBWpcaptotcpgaps.sh dumpfiles.lst dumpfiles/

#get list of directories with .npz files
#from BandWidth/
bash getLists.sh

#reduce npz files into aggregations
#from BandWidth/
bash runETL.sh

#combine reduced records into a single file
#from BandWidth/
python combineReducedRecords.py outdata/

#visualize the data(veeery experimental)
#from BandWidth/
python plot_aggs.py
