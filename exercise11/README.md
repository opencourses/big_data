# Exercise 11

## Description

Write an hadoop program able to calculate the average of a set of meausurements
using the in-mapper comnbiner.

 - Input: a collection of structured textual csv files, containing the daily value
   of pollution measured by a set of sensors.
    Each line of the files has the following format:
   
    sensorId,date,pollution_value

 - Output: report for each sensor the average value of pollution

Pay attention how you calculate the average considering that you should
have more input files and so more mappers.
