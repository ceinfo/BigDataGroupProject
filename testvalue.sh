
# Run With:   ./test.sh NYPD_Complaint_Data_Historic.csv <index,index,index>
# Output:  src/srctmpvalue<cols>.out


# Starting processing for the Hadoop Job
hdfsout="src/srctmpvalue.out"
/usr/bin/hadoop fs -rm -r -f "srctmpvalue.out"
/usr/bin/hadoop jar /opt/cloudera/parcels/CDH-5.9.0-1.cdh5.9.0.p0.23/lib/hadoop-mapreduce/hadoop-streaming.jar -D mapreduce.job.reduces=10 -D mapreduce.map.memory.mb=18384 -D mapreduce.map.java.opts=-Xms10000m  -files "src/" -mapper "src/mapvalue.py $2" -reducer "src/reducevalue.py" -input "$1" -output "srctmpvalue.out"
/usr/bin/hadoop fs -getmerge "srctmpvalue.out" "src/srcvalue$2_output"

