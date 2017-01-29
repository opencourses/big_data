#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise3_data > /dev/null && hdfs dfs -rm -r /exercise3_data
hdfs dfs -ls /exercise3_out > /dev/null && hdfs dfs -rm -r /exercise3_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise3_data" /exercise3_data


# Run application
hadoop jar "$base_dir/target/exercise3-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                1 /exercise3_data  /exercise3_out 40.5
