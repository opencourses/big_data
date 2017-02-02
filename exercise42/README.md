# Exercise 42

## Description

Make a spark program to calculate the 2-gram frequency of a big review dataset.

The dataset is a csv scraped from amazon regarding mobile products.

The dataset is so composted: 

    400 thousand reviews of unlocked mobile phones sold on Amazon.com to find out insights 
    with respect to reviews, ratings, price and their relationships.

The schema is so composed:

    Product Name: String
    Brand Name: String
    Price: Numeric
    Rating: Numeric
    Reviews: String
    Review Votes: Numeric

You can download the sample dataset from [here](https://www.kaggle.com/PromptCloudHQ/amazon-reviews-unlocked-mobile-phones)


## Result:

A part of the result with the given data is:

    (106469,the phone)
    (84011,this phone)
    (43542,of the)
    (42224,it is)
    (38699,phone is)
    (38594,i have)
    (35171,on the)
    (34525,and the)
    (34341,it was)
    (33726,is a)

