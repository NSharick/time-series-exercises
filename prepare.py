## Data Preparation Function for Time Series Module

#imports
import pandas as pd
import numpy as np
import datetime

#Prep function for sales data 
def prep_sales(df):
    '''
    This function takes in the dataframe returned by the stores_items_sales_data function located in the 
    acquire.py file which pulls and joins data from the API. This function removes the unneeded trainling data 
    from the sale_date column values, changes the sale_date column to a datetime object and sets it as the index
    and creates some new columns for data exploration
    '''
    df['sale_date'] = df['sale_date'].str[:-13]
    df['sale_date'] = pd.to_datetime(df.sale_date)
    df = df.set_index('sale_date').sort_index()
    df['month'] = df.index.strftime('%m-%b')
    df['day_of_week'] = df.index.strftime('%w-%A')
    df['sales_total'] = df['sale_amount'] * df['item_price']
    return df

#prep function for the ops data
def prep_opsd(df):
    '''
    This function takes in the dataframe returned by the 'get_opsd' function in the acquire.py file.
    It changes the date column to a datetime type, sets the date as the index, makes 'month' and 'year' columns
    and fills the null value cells with a zero
    '''
    df['Date'] = pd.to_datetime(df.Date)
    df = df.set_index('Date').sort_index()
    df['month'] = df.index.strftime('%m-%b')
    df['year'] = df.index.strftime('%Y')
    df = df.fillna(0)
    return df


