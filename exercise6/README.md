# Exercise 6

## Description

Write an hadoop program able to calculate max and min from a set of pollution measurements.

 - Input: a collection of structured textual csv files containing the daily value of
   pollution measurements for a set of sensors. Each line of the file has the
   following format

    sensorId,date,pollution_value

 - Output: report for each sensor the maximum and the minimum value of pollution.


## Hints
You need to send a lot of data to through the network, so try to implement your
solution with a `combiner` to calculate a local maximum and minimum
