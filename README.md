# Data Scientist Nanodegree

## Spark for Big Data

## Project: Analyzing Customer Churn with Spark

## Table of Contents
1. [Project Overview](#overview)
2. [Project Motivation](#motive)
3. [Data](#data)
4. [Project Components](#components)
    1. [Exploratory Data Analysis](#analysis)
    2. [Feature Engineering](#f_eng)
    3. [Modelling](#model)
5. [Files](#files)
6. [Assumptions](#assume)
***

<a id='overview'></a>

## Project Overview

Imagine you are working on the data team for a popular digital music service
similar to **Spotify** or **Pandora**. Millions of users stream their favorite songs to your service every day either using the free tier that plays advertisements
between the songs or using the **premium** subscription model where they stream
music at free but pay a monthly flat rate. Users can upgrade, downgrade, or
cancel their service at any time so it's crucial to make sure your users love
the service.

Every time a user interacts with the service: whether playing songs, logging
out, liking a song with a thumbs up, hearing an ad or downgrading their service,
**it generates data.**

All this data contains the **key insights** for keeping your users happy and helping
your business thrive. It's your job on the data team to predict which users are
at **risk to churn:** either **downgrading** from premium to free tier or **cancelling**
their service altogether. If you can accurately identify these users before they
leave, your business can offer them **discounts** and **incentives**, potentially saving
your business **millions in revenue.**

To tackle this project we are provided with a large data set ( 12 GB is you deploy
your cluster on **AWS**, 128 MB subset if working on **local machine** ) that contains the
events we just described. You will need to **load, explore** and **clean** this data set
with **Spark**. Based on your exploration you will **create** features and **build** models
with Spark to **predict** which users will churn from your digital music service.
This project is all about demonstrating mastery of Spark **scalable** data
**manipulation** and machine learning. After completing this project, you'll have
built a useful model with a massive data set. You'll be able to apply these same
skills with Spark to wrangle data and build models in your role as a data
scientist.

<a id='motive'></a>

## Project Motivation

You'll learn how to manipulate large and realistic datasets with Spark to
engineer relevant features for **predicting churn.** You'll learn how to use
Spark ML library to build machine learning models with large datasets, far beyond what
could be done with **non-distributed technologies like scikit-learn.**

Predicting churn rates is a challenging and common problem that data scientists
and analysts regularly encounter in any customer-facing business. Additionally,
the ability to efficiently manipulate large datasets with Spark is one of the
highest-demand skills in the field of data.

<a id='data'></a>

## Data

This repository contains a **tiny** subset **(128MB)** of the **full** available
dataset **(12GB).** It contains information about each user like which page
they were on, location, gender, timestamp, etc.

For more information about feature space, see _metadata.xlsx_ in **data** directory.
<a id='components'></a>

## Project Components

There are three parts in this project:

<a id='analysis'></a>

### Exploratory Data Analysis

#### Data Cleaning

<img src="./img/nan_cols.png" alt="nan_cols_count" width="100%">

First we remove null values for some columns. There are two distinct number of
null values observed: 8346 and 58392

58392 is 20% of the data (286500) and 8346 is merely 2%. So we keep the columns which
have 2% `nan`s and see the 20% one's whether we can impute the missing values.

<img src="./img/nan_cols_more.png" alt="nan_cols_count" width="100%">

Seems like it is difficult to fill the missing values. We will drop the
respective rows with null values.

#### Data Analysis

#### Univariate Plots

1. Distribution of pages

<img src="./img/page_dist_2.png" alt="page_dist_2" width="100%">

We will remove the starting classes: `Cancel` and `Cancellation Confirmation` in
our modelling section to avoid lookahead bias.

Most commonly browsed pages include activities like addition to playlist,home
page, and thumbs up.

2. Distribution of levels(free or paid)

<img src="./img/level_dist.png" alt="level_dist" width="100%">

70% of churned users are paying customers.

3. Song length

<img src="./img/songs_dist.png" alt="songs_dist" width="100%">

No additional information is available from this visualisation. It just shows
that most songs are 4 minutes long.

4. What type of device user is streaming from?

<img src="./img/devices_dist.png" alt="devices_dist" width="100%">

We got what we expected. Windows is the most used platform.

#### Multivariate Plots

1. Gender distribution

<img src="./img/gen_dist.png" alt="gen_dist" width="100%">

Males are more in number.

2. Distribution of pages based on churn

<img src="./img/page_churn_dist.png" alt="page_churn_dist" width="100%">

No strong conclusion can be drawn from this graph.

3. Distribution of hour based on churn

<img src="./img/hod_churn_dist.png" alt="hod_churn_dist" width="100%">
<img src="./img/diff_hod_churn_dist.png" alt="diff_hod_churn_dist" width="100%">

We can see non-churn users are more active during day time.

4. Behaviour across weekdays

<img src="./img/dow_churn_dist.png" alt="hod_churn_dist" width="100%">
<a id='f_eng'></a>

Activity is more on weekdays. Especially for churned users.

5. Behaviour at the month level

<img src="./img/dom_churn_dist.png" alt="dom_churn_dist" width="100%">

<img src="./img/diff_dom_churn_dist.png" alt="diff_dom_churn_dist" width="100%">

An interesting pattern is seen here. Churn users are generally more active in
the start of the month as compared to non-churn users, and opposite is the case
at the EOM.

### Feature Engineering

<a id='model'></a>

### Modelling
<a id='files'></a>

## Files

<pre>
.
|
+-+helper.py
</pre>
# Sparkify-capstone
Data Analysis in Spark to Identify Customer Churn for fictional music service(like Spotify)

References
- getting dummies like pandas in pySpark - https://stackoverflow.com/questions/42805663/e-num-get-dummies-in-pySpark
- using multiple if-else conditions in list comprehension - https://stackoverflow.com/questions/9987483/elif-in-list-comprehension-conditionals
- state to region - https://www.businessinsider.in/The-US-government-clearly-defines-the-Northeast-Midwest-South-and-West-heres-where-your-state-falls/THE-MIDWEST/slideshow/63954185.cms
- deleting columns in pySpark - https://stackoverflow.com/questions/29600673/how-to-delete-columns-in-pySpark-dataframe

Divide states into regions -
https://www.businessinsider.in/The-US-government-clearly-defines-the-Northeast-Midwest-South-and-West-heres-where-your-state-falls/articleshow/63954190.cms
Write single CSV file using spark-csv- https://stackoverflow.com/questions/31674530/write-single-csv-file-using-spark-csv