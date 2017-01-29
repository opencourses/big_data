#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /ex_name_data > /dev/null && hdfs dfs -rm -r /ex_name_data
hdfs dfs -ls /ex_name_out > /dev/null && hdfs dfs -rm -r /ex_name_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/ex_name_data" /ex_name_data


# Run application
hadoop jar "$base_dir/target/ex_name-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                1 /ex_name_data  /ex_name_out
