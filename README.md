# Udacity-Data-Engineering-Capstone
This project aims to combine four data sets containing immigration data, airport codes, demographics of US cities and global temperature data. The primary purpose of the combination is to create a schema which can be used to derive various correlations, trends and analytics. For example, one could attempt to correlate the influence of the average temperature of a migrant's resident country on their choice of US state, and what the current demographic layout of that state is.

## Datasets:
- i94 Immigration Sample Data: Sample data of immigration records from the US National Tourism and Trade Office. This data source will serve as the Fact table in the schema. This data comes from https://travel.trade.gov/research/reports/i94/historical/2016.html.
- World Temperature Data world_temperature. This dataset contains temperature data in various cities from the 1700’s to 2013. Although the data is only recorded until 2013, we can use this as an average/gauge of temperature in 2017. This data comes from https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data.
- US City Demographic Data: Data about the demographics of US cities. This dataset includes information on the population of all US cities such as race, household size and gender. This data comes from https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/.
- Airport Codes: This table contains the airport codes for the airports in corresponding cities. This data comes from https://datahub.io/core/airport-codes#data.

## Data Model:
In accordance with Kimball Dimensional Modelling Techniques, laid out in this document (http://www.kimballgroup.com/wp-content/uploads/2013/08/2013.09-Kimball-Dimensional-Modeling-Techniques11.pdf), the following modelling steps have been taken:

- Select the Business Process:
    The immigration department follows their business process of admitting migrants into the country. This process generates events which are captured and translated to facts in a fact table
- Declare the Grain:
    The grain identifies exactly what is represented in a single fact table row.
    In this project, the grain is declared as a single occurrence of a migrant entering the USA.
- Identify the Dimensions:
    Dimension tables provide context around an event or business process.
    The dimensions identified in this project are:
        dim_migrant
        dim_status
        dim_visa
        dim_temperature
        dim_country
        dim_state
        dim_time
        dim_airport
- Identify the Facts:
    Fact tables focus on the occurrences of a singular business process, and have a one-to-one relationship with the events described in the grain.
    The fact table identified in this project is:
        fact_immigration

For this application, I have developed a set of Fact and Dimension tables in a Relational Database Management System to form a Star Schema. 


## 3.2 Mapping Out Data Pipelines
List the steps necessary to pipeline the data into the chosen data model:
- Load the data into staging tables
- Create Dimension tables
- Create Fact table
- Write data into parquet files
- Perform data quality checks

## Project Write Up:
1. Clearly state the rationale for the choice of tools and technologies for the project:
- This project makes use of various Big Data processing technologies including:
  - Apache Spark, because of its ability to process massive amounts of data as well as the use of its unified analytics engine and convenient APIs
  - Pandas, due to its convenient dataframe manipulation functions
  - Matplotlib, to plot data and gain further insights
2. Propose how often the data should be updated and why:
 - The immigration (i94) data set is updated monthly, hence all relevant data should be updated monthly as well
3. Write a description of how you would approach the problem differently under the following scenarios:
3.1 The data was increased by 100x:
 - If the data was increased by 100x I would use more sophisticated and appropriate frameworks to perform processing and storage functions, such as Amazon Redshift, Amazon EMR or Apache Cassandra.
3.2 The data populates a dashboard that must be updated on a daily basis by 7am every day:
 - If the data had to populate a dashboard daily, I would manage the ETL pipeline in a DAG from Apache Airflow. This would ensure that the pipeline runs in time, that data quality checks pass, and provide a convenient means of notification should the pipeline fail.
3.3 The database needed to be accessed by 100+ people:
 - If the data needed to be accessed by many people simultaneously, I would move the analytics database to Amazon Redshift which can handle massive request volumes and is easily scalable.