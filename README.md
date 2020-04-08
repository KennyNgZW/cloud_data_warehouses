# Project Summary
Creation, Data Extration and Data Loading of Amazon Redshift data warehouse with Star schema for music streaming startup, Sparkify to perform efficient analysis.

# Project Description and Purpose
With the growing size of data along with the growth of the startup, moving processes and data onto the cloud become a must. That's because this would bring lower cost but more efficient operation.

The reason to create and build data pipeline to the database in cloud is because, to boost the development of the startup, we need to focus on the increasing amount of data and react efficiently as the data changes. The cloud services from AWS provide us with a safe and cloud-managed database and a low price.

Moreover, with the Star schema and different distribution strategies, this database can serve Sparkify' aim to analyse "what songs their users are listening to". As the star schema optimise the query operation by eliminating the complexity of 3NF and show understandable results in fact table directly. Also, with appropriate distribution strategies, when queries are operated, JOINs will be more time-saving.

# ETL Pipeline
Extraction, transform and loading (ETL) is the necessary process before data can be loaded into a database. This process converts the source data into different forms to fit the requirement in the destination system.

## ETL - Extract Step
As the two sets of data are stored in S3 bucket, data is extracted using COPY command to perform faster retrieval and loaded into staging tables.

## ETL - Transform Step
Other than timestamp, the other data can be loaded into staging tables without transform. However, we need the details within the timestamp. So, the timestamp for each song play (start time) in milliseconds format is transformed into datetime format before loading so that we can get start_time in datetime format, hour, day, week, month, year and weekday.

## ETL - Load Step
In the load step, the extracted and transformed data is loaded into the dimension and fact tables.

# Results
As shown after running etl.py, there are 957 records in songplay fact table, 59584 records in song dimension table, 38212 records in artist dimension table, 388 in user dimension table and finally 32092 in time dimension table.