
# Run With:   ./test.sh NYPD_Complaint_Data_Historic.csv <index,index,index>
# Output:  src/output


# Starting processing for the Hadoop Job
hdfsout="src/srctmp.out"
/usr/bin/hadoop fs -rm -r -f "srctmp.out"
/usr/bin/hadoop jar /opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=10 -D mapreduce.map.memory.mb=18384 -D mapreduce.map.java.opts=-Xms10000m  -files "src/" -mapper "src/map.py $2" -reducer "src/reduce.py" -input "$1" -output "srctmp.out"
/usr/bin/hadoop fs -getmerge "srctmp.out" "src/srctmp.out"
grep "Data" $hdfsout | sort -n > "src/output"; grep "Sem" $hdfsout | sort -n >> "src/output"; grep "Valid" $hdfsout | sort -n  >> "src/src$2_output"

