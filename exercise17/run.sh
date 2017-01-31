#!/bin/bash
set -e 

base_dir=$(cd `dirname "$0"` && pwd -P)

# This script moves the files in the filesystem and then run hadoop
# Remove folders of the previous run
hdfs dfs -ls /exercise17_data > /dev/null && hdfs dfs -rm -r /exercise17_data
hdfs dfs -ls /exercise17_out > /dev/null && hdfs dfs -rm -r /exercise17_out

# Put input data collection into hdfs
hdfs dfs -copyFromLocal "$base_dir/exercise17_data" /exercise17_data


# Run application
hadoop jar "$base_dir/target/exercise17-1.0.0.jar" com.alangiu.bigdata.hadoop.DriverImpl \
                                                    /exercise17_data  /exercise17_out
