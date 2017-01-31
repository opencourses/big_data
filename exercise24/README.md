# Exercise 24

## Description

Write an hadoop program that is able to convert a text to a set of integers.

 - Input: A large textual file containing a list of words per line and 
   a small file (dictionary.txt) containing the mapping of each possible word appearing
   in the first file with an integer. Each line contain the mapping of a word with an
   integer an it has the following format

    `Word\tInteger\n`

 - Output: a textual file containing the content of the large file where the 
   appearing words are substituted by the corresponding integers.

The output must preserve the line order of the input;
