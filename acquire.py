## Data Acquisition Functions for Time Series Exercises

#imports
import pandas as pd
import requests
import os

## Function for acquiring data from the codeup api
def get_data(domain, endpoint, query_str, top_key, section_key, maxpage_key):
    '''
    This function takes in the domain(including the protocol), the endpoint, and the query string for the api
    and also takes in the top level key and the section key for where the data is located as well as 
    a key that will return the max number of pages with data that needs to be pulled. The function returns 
    a dataframe with the data from all pages stacked as rows. The function also checks to see if the dataset is 
    already saved as a csv in the local directory and will read from it if it is or will save a copy after the pull if it is not 
    '''
    #assign the file name for the csv
    filename = section_key + '.csv'
    #check if the csv exists in the local directory
    if os.path.exists(filename):
        print('Reading from csv file...')
        #read the local .csv into the notebook
        df = pd.read_csv(filename)
        return df
    print('Getting a fresh copy from the api...')
    #create an empty list
    items = []
    #assign the domain and endpoint as the initial url
    url = domain + endpoint
    #make the request to the website
    response = requests.get(url)
    #take the return data and write it into the notebook in json format
    data = response.json()
    #append the empty list with the list of dictionaries returned from the website
    items.extend(data[top_key][section_key])
    #assign the max pages value using the key from the api
    max_pages = (data[top_key][maxpage_key])+1
    #create a loop to loop through all the pages of data available for the category in the api
    for page in range(2, max_pages):
        url = domain + endpoint + query_str + str(page)
        #print the url so the functions progress can be tracked
        print(url)
        response = requests.get(url)
        data = response.json()
        items.extend(data[top_key][section_key])
    #change the items list to a dataframe
    df = pd.DataFrame(items)
    #save the dataframe as a csv int he local directory
    df.to_csv(filename, index=False)
    return df

##function to put the returned dataframes together in a single dataframe
def stores_items_sales_data():
    stores_df = get_data('https://api.data.codeup.com', '/api/v1/stores', '?page=', 'payload', 'stores', 'max_page')
    items_df = get_data('https://api.data.codeup.com', '/api/v1/items', '?page=', 'payload', 'items', 'max_page')
    sales_df = get_data('https://api.data.codeup.com', '/api/v1/sales', '?page=', 'payload', 'sales', 'max_page')
    sales_df = sales_df.rename(columns={'item': 'item_id', 'store': 'store_id'})
    df = pd.merge(sales_df, items_df, how='left', on='item_id')
    df = pd.merge(df, stores_df, how='left', on='store_id')
    return df