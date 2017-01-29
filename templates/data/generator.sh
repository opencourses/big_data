#!/bin/bash
set -e 

# Configure this parameter to get the desidered file
numberoflines=10
# you can pass the number of lines with the command line
if [ "$1"x != ""x ]; then
    numberoflines=$1
fi

# FORMAT
# The possible formats are:
#   - $date     : indicates a random date between the setted values
#   - $n1       : is a random number between min1 and max1
#   - $n2       : is a random number between min2 and max2
#   - $n3       : is a random number between min3 and max3
#   - $n4       : is a random number between min4 and max4
#   To generate a double number simply use $1.$2 setting the max for the two values
format='s$n1,$date   $n2.$n3'

# NUMBERS
min1=0
max1=30

min2=0
max2=80

min3=0
max3=9

min4=0
max4=30

# DATE
startdate=20160512
enddate=20160913
dateformat="%Y-%m-%d"

function run {
    i=0;
    while [ $i -lt $numberoflines ]; do
        let i=$i+1
        date=$(get_date)
        n1=$(get_random $min1 $max1)
        n2=$(get_random $min2 $max2)
        n3=$(get_random $min2 $max3)
        n4=$(get_random $min3 $max4)
        out=$(eval "echo $format")
        echo $out
    done
}

## DATE GENERATOR
diffdata=$(( ($(date -j -f "%Y%m%d" $enddate +%s) - $(date -j -f "%Y%m%d" $startdate +%s) )/(60*60*24) ))
function get_date() {
    j=$(get_random 0 $diffdata)
    currentdate=`date   -j -v+${j}d  -f "%Y%m%d" "$startdate" +$dateformat`
    echo $currentdate
}

## RANDOM NUMBER GENERATOR
function get_random() {
    min=${1-0}
    max=${2-""}
    if [ "$max"x != ""x ]; then
        echo $((($RANDOM%$max+$min)))
    else
        echo $((($RANDOM+$min)))
    fi
}

run
