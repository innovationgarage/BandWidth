#get list of directories with dumpfiles
find ./data -name '*interfaces*' -printf "%h\n" | sort -u > lst
#convert dumpfiles into npz files using "interfaces"
bash sameLBWpcaptotcpgaps.sh lst dumpfiles/
#reduce npz files into aggregations
bash runETL.sh dumpfiles
#combine reduced records into a single file
python combineReducedRecords.py outdata/
