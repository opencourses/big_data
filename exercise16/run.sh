#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise16_data1 > /dev/null && hdfs dfs -rm -r /exercise16_data1
hdfs dfs -ls /exercise16_data2 > /dev/null && hdfs dfs -rm -r /exercise16_data2
hdfs dfs -ls /exercise16_out > /dev/null && hdfs dfs -rm -r /exercise16_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise16_data1" /exercise16_data1
hdfs dfs -copyFromLocal "$base_dir/exercise16_data2" /exercise16_data2


# Run application
hadoop jar "$base_dir/target/exercise16-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                1 /exercise16_data1 /exercise16_data2  /exercise16_out
