#!/bin/bash -e
src=false

if [[ $_ != $0 ]]; then
    src=true	
fi

base_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Script configurations:
ex_name="__EXERCISE_NAME__"
jar="$base_dir/target/$ex_name-1.0.0.jar"
class="com.alangiu.bigdata.spark.SparkDriver"

## path configurations
## this will be the folders inside the HDFS. Please use absolute paths
local_data_dir=$base_dir/$ex_name"_data"
data_dirs=/$ex_name"_data"
out_dirs=/$ex_name"_out"

args="$data_dirs/data.txt $out_dirs"
local_args="$local_data_dir $base_dir/$out_dirs"


# Remove folders of the previous run
clean_data() {
    hdfs dfs -rm -r $data_dirs
}

clean_result() {
    hdfs dfs -rm -r $out_dirs
}

# Put input data collection into hdfs
copy() {
    echo "Copying data to HDFS"
    hdfs dfs -copyFromLocal $local_data_dir $data_dirs
}

# Run application
submit() {
    spark-submit  --class $class --deploy-mode cluster \
                  --master yarn $jar $args
}

run_local() {
    spark-submit  --class $class --deploy-mode client \
                  --master local $jar $local_args
}

run() {
    clean_data
    clean_result
    copy
    submit
}

out() {
    hdfs dfs -cat $out_dirs/*
}

if [ $src = false ]; then
    cmd=$1
    shift
    case "$cmd" in
        clean_data)
            clean_data
            ;;
        clean_result)
            clean_result
            ;;
        clean)
            clean_data
            clean_result
            ;;
        copy)
            copy
            ;;
        submit)
            submit
            ;;
        run)
            run
            ;;
        out)
            out
            ;;
        *)
            # If the script has been sourced it means that we want to call the 
            # respective functions direcly in the command line, so don't print
            # the log message
            [[ $_ != $0 ]]; exit 1
            echo "Spark application runner"
            echo "---- Available commands are:"
            echo "----- clean"
            echo "----- clean_data"
            echo "----- clean_result"
            echo "----- copy"
            echo "----- submit"
            echo "----- run"
    esac
fi

export -f run
export -f run_local
export -f out
export -f copy
export -f submit
export -f clean_data
export -f clean_result
