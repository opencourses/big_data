# Exercise 18

## Description

Write an hadoop program that is able to split the readings of a set of sensors based
on the value of the maasurement.

 - Input: a set of textual files containing the temperatures gathered by a set of 
   sensors. Each line of the files has the following format:

    `sensorID,date,hour,temperature\n`

 - Output: a set of files with the prefix *"high-temp-"* containing the lines
   of the input files with a temperature value greater than 30.0 and a set of 
   files with the prefix *"normal-temp-"* containing the lines of the input files
   with a temperature value less than or equal to 30.0
