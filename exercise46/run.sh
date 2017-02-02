#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise46_data > /dev/null && hdfs dfs -rm -r /exercise46_data
hdfs dfs -ls /exercise46_out > /dev/null && hdfs dfs -rm -r /exercise46_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise46_data" /exercise46_data


# Run application
hadoop jar "$base_dir/target/exercise46-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                1 /exercise46_data/BoughtBooks.txt  /exercise46_out
