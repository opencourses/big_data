# Exercise 12

## Description

Make an hadoop program able to select the ouliers in a file containing measurements.

 - Input: a collection of structured textual files containing the daily value of pollution
   for a set of sensors

   Each line of the file has the following format:
   
   `sensorId,date\tpollution_value`
   where \t is the tab char.

 - Output: the records with a pollution value below a user provided threshold.
   The threshold is an argument of the program
