# Import Libraries
import pandas as pd
import numpy as np
import missingno as msno
pd.set_option('display.width',170, 'display.max_rows',200, 'display.max_columns',900)
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *

# Add functions as needed to perform necessary operations
def print_dataframe_info(df):
    print ("\n\n---------------------")
    print ("DATAFRAME INFORMATION")
    print ("---------------------")
    print ("Dataframe shape:", df.shape, "\n")
    print ("Column Headers:", list(df.columns.values), "\n")
    print (df.dtypes)
    
def print_dataframe_report(df):
    import re
    
    missing = []
    non_numeric = []
    
    for column in df:
        # find unique values
        values = df[column].unique() 
        st = "{} has {} unique values".format(column, values.size)
        print(st)
        
        if (values.size > 10):
            print("Listing up to 10 unique values:")
            print(values[0:10])
            print ("\n---------------------\n")
            
        # find missing values in features
        if (True in pd.isnull(values)):
            percentage = 100 * pd.isnull(df[column]).sum() / df.shape[0]
            s = "{} is missing in {}, {}%.".format(pd.isnull(df[column]).sum(), column, percentage)
            missing.append(s)

        # find non-numeric values in features
        for i in range(1, np.prod(values.shape)):
            if (re.match('nan', str(values[i]))):
                break
            if not (re.search('(^\d+\.?\d*$)|(^\d*\.?\d+$)', str(values[i]))):
                non_numeric.append(column)
                break
    print ("\n~~~~~~~~~~~~~~~~~~~~~~\n")
    
    print ("Features with missing values:")
    for i in range(len(missing)):
        print("\n{}" .format(missing[i]))
        
    print ("\nFeatures with non-numeric values:")
    print("\n{}" .format(non_numeric))
        
    print ("\n~~~~~~~~~~~~~~~~~~~~~~\n")
    
def eliminate_missing_data(df):
    """
        Clean the data within the dataframe.
        
        :param df: dataframe
        :return: the cleaned dataframe
    """
    print("Dropping missing data...")
    
    columns_to_drop = []
    
    for column in df:
        values = df[column].unique() 
        
        # find missing values in features
        if (True in pd.isnull(values)):
            percentage_missing = 100 * pd.isnull(df[column]).sum() / df.shape[0]
            
            if (percentage_missing >= 90):
                columns_to_drop.append(column)
    
    # Drop columns missing 90% or more of their values
    df = df.drop(columns=columns_to_drop)
    
    # drop rows where all elements are missing
    df = df.dropna(how='all')
    
    print("Cleaning complete!")
    
    return df

def drop_duplicate_rows(df, cols=[]):
    """
        Drop duplicate data within the dataframe.
        
        :param df: dataframe
        :return: the cleaned dataframe
    """
    print("Dropping duplicate rows...")
    row_count_before = df.shape[0]
    # drop duplicate rows
    df = df.drop_duplicates()
    print("{} rows dropped.".format(row_count_before - df.shape[0]))
    return df

def write_to_parquet(df, output_path, table_name):
    """
        Writes the dataframe as parquet file.
        
        :param df: dataframe to write
        :param output_path: output path where to write
        :param table_name: name of the table
    """
    
    file_path = output_path + table_name
    
    print("Writing table {} to {}".format(table_name, file_path))
    
    df.write.mode("overwrite").parquet(file_path)
    
    print("Write complete!")
    
def perform_quality_check(input_df, table_name):
    """Check data completeness by ensuring there are records in each table.
        :param input_df: spark dataframe to check counts on.
        :param table_name: name of table
    """
    
    record_count = input_df.count()

    if (record_count == 0):
        print("Data quality check failed for {} with zero records!".format(table_name))
    else:
        print("Data quality check passed for {} with record_count: {} records.".format(table_name, record_count))
        
    return 0
    
    