#get list of directories with dumpfiles
bash getLists.sh
#convert dumpfiles into npz files using "interfaces"
bash sameLBWpcaptotcpgaps.sh lst dumpfiles/
#reduce npz files into aggregations
bash runETL.sh dumpfiles
#combine reduced records into a single file
python combineReducedRecords.py outdata/
#visualize the data(veeery experimental)
python plot_aggs.py
