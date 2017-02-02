#!/bin/bash
set -e 

# Configure this parameter to get the desidered file
numberoflines=1000
# you can pass the number of lines with the command line
if [ "$1"x != ""x ]; then
    numberoflines=$1
fi

# FORMAT
# The possible formats are:
#   - $date     : indicates a random date between the setted values
#   - $words    : is a random phrase generated combining 'nwords' words
#   - $n1       : is a random number between min1 and max1
#   - $n2       : is a random number between min2 and max2
#   - $n3       : is a random number between min3 and max3
#   - $n4       : is a random number between min4 and max4
#   - $inc1     : is an incremental number starting from inc1+1
#   - $ip       : is a random ip address
#   - $t        : is the tabulation
#   To generate a double number simply use $1.$2 setting the max for the two values
format='A$n1,Q$n2,$date,$words'

# NUMBERS
min1=1
max1=1000

min2=1
max2=100

min3=0
max3=9

min4=0
max4=30

inc1=0

# DATE
startdate=20160512
enddate=20160913
sequencial=false
dateformat="%Y-%m-%d"

# RANDOM WORD PICKER
nwordsmin=5
nwordsmax=10
outwords="There are questions without answer. Because I do it. I don't know"


## MAIN METHOD
t='\t'
dateb=false
wordsb=false
n1b=false
n2b=false
n3b=false
n4b=false
inc1b=false
ipb=false
if [[ $format == *"n1"* ]]; then
    n1b=true;
fi
if [[ "$format" == *"n2"* ]]; then
    n2b=true;
fi
if [[ "$format" == *"n3"* ]]; then
    n3b=true;
fi
if [[ "$format" == *"n4"* ]]; then
    n4b=true;
fi
if [[ "$format" == *"inc1"* ]]; then
    inc1b=true;
fi
if [[ "$format" == *"date"* ]]; then
    dateb=true;
fi
if [[ "$format" == *"words"* ]]; then
    wordsb=true;
fi
if [[ "$format" == *"ip"* ]]; then
    ipb=true;
fi
function run {
    i=0;
    while [ $i -lt $numberoflines ]; do
        let i=$i+1
        if [ $dateb = true ]; then
            date=$(get_date $i)
        fi
        if [ $wordsb = true ]; then
            words=$(get_words)
        fi
        if [ $n1b = true ]; then
            n1=$(get_random $min1 $max1)
        fi
        if [ $n2b = true ]; then
            n2=$(get_random $min2 $max2)
        fi
        if [ $n3b = true ]; then
            n3=$(get_random $min2 $max3)
        fi
        if [ $n4b = true ]; then
            n4=$(get_random $min3 $max4)
        fi
        if [ $inc1b = true ]; then
            let inc1=inc1+1
        fi
        if [ $ipb = true ]; then
            ip=$(get_ip)
        fi
        eval "echo -e $format"
    done
}

## DATE GENERATOR
diffdata=$(( ($(date -j -f "%Y%m%d" $enddate +%s) - $(date -j -f "%Y%m%d" $startdate +%s) )/(60*60*24) ))
j=0
function get_date() {
    if [ $sequencial = true ]; then
        j=$1
    else 
        j=$(get_random 0 $diffdata)
    fi
    currentdate=`date   -j -v+${j}d  -f "%Y%m%d" "$startdate" +$dateformat`
    echo $currentdate
}

## WORDS GENERATOR
wordc=$(echo $outwords | wc -w)
function get_words() {
    if [ $nwordsmin == $nwordsmax ]; then
        i=$nwordsmin
    else 
        i=$(get_random $nwordsmin $nwordsmax)
    fi
    while [ $i -ge "$nwordsmin" ]; do
        let i=i-1
        j=$(get_random 1 $wordc)
        echo $outwords | cut -d " " -f $j
    done
}

## RANDOM NUMBER GENERATOR
function get_random() {
    min=${1-0}
    max=${2-""}
    if [ "$max"x != ""x ]; then
        echo $(((($RANDOM%($max-$min))+$min)))
    else
        echo $((($RANDOM+$min)))
    fi
}

function get_ip() {
    ip1=$(get_random 1 255)
    ip2=$(get_random 1 255)
    ip3=$(get_random 1 255)
    ip4=$(get_random 1 255)
    echo "$ip1.$ip2.$ip3.$ip4"
}

run
