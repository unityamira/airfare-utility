import requests 
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import re
import string

def get_nameslist(df):
    origins = df['Origination']
    destinations = df['Destination']

    locations = pd.concat([origins, destinations])
    locations = locations.drop_duplicates().reset_index(drop=True)

    new_df = pd.DataFrame(locations, columns = ['Location'])

    new_df.to_csv("namelist.csv")

    return new_df

def check_names(df):

    new_df = df
    new_df['Input'] = np.nan
    new_df['Result'] = np.nan
    
    for i in new_df.index:

        print(new_df['Location'][i])
        if (new_df['Location'][i]) == "LAX":
            continue 

        #case where it works the first time 
        input = new_df['Location'][i]
        result = get_miles(input, "LAX")[0]

        if (result != 9999) & (result != 0):
            new_df['Result'][i] = 7777
            new_df['Input'][i] = input

        #try city name 
        input = new_df['Location'][i].split(",")[0]
        result = get_miles(input, "LAX")[0]

        if (result != 9999) & (result != 0):
            new_df['Result'][i] = 7777
            new_df['Input'][i] = input
        else:
            input = new_df['Location'][i].split()[0]
            result = get_miles(input, "LAX")[0]

        if (result != 9999) & (result != 0):
            new_df['Result'][i] = 7777
            new_df['Input'][i] = input
        else:
            input = new_df['Location'][i].split("-")[0]
            result = get_miles(input, "LAX")[0]

        if (result != 9999) & (result != 0):
            new_df['Result'][i] = 7777
            new_df['Input'][i] = input
        else:
            input = new_df['Location'][i].split("/")[0]
            result = get_miles(input, "LAX")[0]

        if (result != 9999) & (result != 0):
            new_df['Result'][i] = 7777
            new_df['Input'][i] = input
        else: 
            new_df['Result'][i] = result
            new_df['Input'][i] = input

    new_df.to_csv("name_check.csv")

    return new_df

def clean_names(df, column):
    #replace - , / with commas
    new_df = df

    primary_airport = pd.read_csv("primary_airport.csv")
    name_in = pd.read_csv("name_check.csv")
    name_check = name_in[['Location', 'Input']]

    new_df = new_df.merge(primary_airport, how="left", left_on=column, right_on='Location')
    new_df = new_df.merge(name_check, how="left", left_on=column, right_on ="Location")

    col = column + "_Cleaned"
    new_df[col] = np.nan
    new_df['has_code'] = new_df.notnull()['Primary_Airport']

    #drop unnecessary, use .not_null() to make other col + them proceed

    for i in new_df.index:
        if (new_df['has_code'][i]):
            new_df[col][i] = new_df['Primary_Airport'][i]
        elif (new_df[column][i] == "LAX"):
             new_df[col][i] = "LAX"
        else:
            new_df[col][i] = new_df['Input'][i]
    
    new_df = new_df.drop(columns = ['Location_x','Location_y', "Input", "has_code", "Primary_Airport"])

    new_df.to_csv("check_merge.csv")

    return new_df
    
def calculate_all_distances(df):
    
    new_df = clean_names(df, "Origination")
    new_df = clean_names(df, "Destination")
    new_df = df
    new_df['One_way'] = np.nan
    new_df['Round_Trip'] = np.nan

    for i in new_df.index:

        print(new_df['Origination_Cleaned'][i])
        print(new_df['Destination_Cleaned'][i])

        miles = get_miles(new_df['Origination_Cleaned'][i], new_df['Destination_Cleaned'][i])
        new_df['One_way'][i] = miles[0]
        new_df['Round_Trip'][i] = miles[1]
    
    new_df.to_csv("all_data_miles.csv")

def get_miles(orig, dest):
    
    if orig == np.nan or dest == np.nan:
        return np.nan

    URL = "http://www.webflyer.com/travel/mileage_calculator/getmileage.php?city=" + orig + "&city=" + dest + "&city=&city=&city=&city=&bonus=0&bonus_use_min=0&class_bonus=0&class_bonus_use_min=0&promo_bonus=0&promo_bonus_use_min=0&min=0&min_type=m&ticket_price="

    req = requests.get(URL)
    soup = BeautifulSoup(req.text)

    #try catch for successful entry
    try:
        one_way = float(soup.find_all(class_ = 'row_header_bg')[1].find_next_sibling(class_ = "row_odd_bg").find_all('span')[1].string.split()[0])
    except:
        if (soup.find(class_ = "subheader").i.string == "INPUT ERROR..."):
            return (0,0)
        else:
            return (9999,9999)

    round_trip = float(soup.find_all(class_ = 'row_header_bg')[2].find_next_sibling(class_ = "row_odd_bg").find_all('span')[1].string.split()[0])

    return (one_way, round_trip)

if __name__ == "__main__":

    #file IO, encoding handles characters missing from UTF-8
    in_file = "all_airfare_data.csv"
    in_df = pd.read_csv(in_file, encoding="latin-1")

    #run
    #calculate_all_distances(in_df)
    #print(get_miles("ONT", "SFO"))

    #namelist = get_nameslist(in_df)
    #check_names(namelist)

    calculate_all_distances(in_df)


