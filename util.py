import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
import configparser
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *


    
    
def clean_immigration(df):
    """Clean immigration dataframe
    Args:
         df: dataframe with monthly immigration data
    Return: 
    clean dataframe
    """
    # EDA has shown these columns to exhibit over 90% missing values, and hence we drop them
    drop_columns = ['occup', 'entdepu','insnum']
    df = df.drop(columns=drop_columns)

    # drop rows where all elements are missing
    df = df.dropna(how='all')

    return df


def clean_spark_immigration_data(df):
    """Clean immigration dataframe
    Args:
        df: spark dataframe with monthly immigration data
    Return:
        clean dataframe
    """
    total_records = df.count()
    
    print(f'Total records in dataframe: {total_records:,}')
    
    # EDA has shown these columns to exhibit over 90% missing values, and hence we drop them
    drop_columns = ['occup', 'entdepu','insnum']
    df = df.drop(*drop_columns)
    
    # drop rows where all elements are missing
    df = df.dropna(how='all')

    new_total_records = df.count()
    
    print(f'Total records after cleaning: {new_total_records:,}')
    
    return df


def create_calendar_dim_table(df):
    return df.count()
    
def clean_temperature_data(df):
    """Clean global temperatures dataset
    
    Args:
        df: pandas dataframe representing global temperatures
    Return: 
        clean dataframe
    """
    # drop rows with missing average temperature
    df = df.dropna(subset=['AverageTemperature'])
    
    # drop duplicate rows
    df = df.drop_duplicates(subset=['dt', 'City', 'Country'])
    
    return df


def clean_spark_temperature_data(df):
    """Clean global temperatures dataset
    
    Args:
        df: spark dataframe representing global temperatures
    Return:
        clean dataframe
    """
    total_records = df.count()
    
    print(f'Total records in dataframe: {total_records:,}')
    
    # drop rows with missing average temperature
    df = df.dropna(subset=['AverageTemperature'])
    
    total_recs_after_dropping_nas = df.count()
    print('Total records after dropping rows with missing values: {:,}'.format(total_records-total_recs_after_dropping_nas))
    
    # drop duplicate rows
    df = df.drop_duplicates(subset=['dt', 'City', 'Country'])
    print('Rows dropped after accounting for duplicates: {:,}'.format(total_recs_after_dropping_nas-df.count()))
    
    return df

def aggregate_temperature_data(df):
    """Aggregate clean temperature data at country level
    
    Args:
        df: spark dataframe of clean global temperaturs data
    Return: 
        spark dataframe consisting of countries average temperatures
    """
    new_df = df.select(['Country', 'AverageTemperature']).groupby('Country').avg()
    
    new_df = new_df.withColumnRenamed('avg(AverageTemperature)', 'average_temperature')
    
    return new_df

def clean_demographics_data(df):
    """Clean the US demographics dataset
    
    :param df: pandas dataframe of US demographics dataset
    :return: clean dataframe
    """
    # drop rows with missing values
    subset_cols = [
        'Male Population',
        'Female Population',
        'Number of Veterans',
        'Foreign-born',
        'Average Household Size'
    ]
    df = df.dropna(subset=subset_cols)
    
    # drop duplicate columns
    df = df.drop_duplicates(subset=['City', 'State', 'State Code', 'Race'])
    
    return df


