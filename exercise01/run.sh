#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)
# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise1_data && hdfs dfs -rm -r /exercise1_data
hdfs dfs -ls /exercise1_out && hdfs dfs -rm -r /exercise1_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise1_data" /exercise1_data


# Run application
hadoop jar "$base_dir/target/exercise1-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl 1 /exercise1_data/lorem  /exercise1_out




