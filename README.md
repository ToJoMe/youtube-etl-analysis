# youtube-etl-analysis

This project uses Python to extract data from two CSV files containing information about YouTube videos and their corresponding comments. The data includes video name, date, ID, number of likes, number of comments, number of views, and keywords for each video. The comment data includes video ID, comment text, likes, and sentiment.

The ETL (Extract, Transform, Load) process is used to clean and transform the data before loading it into a Postgres SQL database. Once the data is loaded, SQL queries are used to analyze the data and gain insights.

## Data Sources

The two CSV files used in this project are:

* videos.csv: contains information about YouTube videos such as the video name, date, ID, number of likes, number of comments, number of views, and keywords.
* comments.csv: contains information about the comments corresponding to each video such as video ID, comment text, likes, and sentiment.


Link to dataset: https://www.kaggle.com/datasets/advaypatil/youtube-statistics

## ETL Process

The ETL process consists of the following steps:

* Extract: The data is extracted from the CSV files using Python's pandas library.
* Transform: The data is cleaned and transformed to remove any inconsistencies and prepare it for loading into the database. This includes renaming columns, dropping irrelevant columns, and converting data types if necessary.
* Load: The transformed data is loaded into a Postgres SQL database using Python's psycopg2 library.

## SQL Analysis

After loading the data into the Postgres SQL database, SQL queries are used to analyze the data and gain insights. The following types of queries are used:

* Filtering: Queries are used to filter the data based on specific criteria, such as videos with a certain number of views or comments.
* Aggregation: Queries are used to aggregate the data and calculate metrics such as the average number of views or likes per video.
* Joining: Queries are used to join the video and comment tables together based on their common video ID column.