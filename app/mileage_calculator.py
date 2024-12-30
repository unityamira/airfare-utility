import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
import os

from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)

from app.db import get_db

#need the 3rd argument or no?
bp = Blueprint('mileage_calculator', __name__ )

#TODO: db connection, db alias normalization, put in round trip support, column selection in finish, enforce constraints in retry,
#figure out concurrency and temp file deletion/ anonymous sessions,  make certain pages inacessible without prior steps, login for admin pages 
# make errors reset 

@bp.route('/', methods=('GET', 'POST'))
#TODO: db connection for aliases
def index():
    #takes uploaded csv, processes it against names database, and passes it to retry endpoint
    if request.method == 'POST':

        csv = request.files['startfile']
        alias_csv = request.files['aliases']
        origination_col = request.form['origination']
        destination_col = request.form['destination']

        session['origination_col'] = origination_col
        session['destination_col'] = destination_col
        session['csv_name'] = csv.filename

        #csv and aliases to pd dfs
        csv.save(csv.filename)
        alias_csv.save(alias_csv.filename)

        df = pd.read_csv(csv.filename)
        aliases = pd.read_csv(alias_csv.filename)

        #error throws 
        error = None
        if not origination_col:
            error = 'Origination is required.'
        elif not destination_col:
            error = 'Destination is required.'
        elif not csv:
            error = 'Please upload a CSV to continue'
        elif not set([origination_col]).issubset(df.columns):
            error = "UPLOAD ERROR: Origination column name not in file, please check name and resubmit"
        elif not set([destination_col]).issubset(df.columns):
            error = "UPLOAD ERROR: Destination column name not found in file, please check name and resubmit"
        elif (df[origination_col].isna().any()):
            error = "UPLOAD ERROR: Origination column has null values, please resubmit csv"
        elif (df[destination_col].isna().any()):
            error = "UPLOAD ERROR: Destination column has null values, please resubmit csv"

        #also add case checking that column names are in the df 

        if error is None:

            names = get_names(df, origination_col, destination_col)
            namekey = pd.merge(names, aliases, how="left", on=['alias'])
            missing = namekey[namekey.iata_code.isnull()][['location','iata_code']]
            missing.to_csv("app/static/missing.csv", index=False)

            if(missing.empty):
                csv_clean = clean_spreadsheet(csv, namekey)
                return_df = calculate_distances(csv_clean)
                return redirect(url_for("mileage_calculator.finish"))
    
            else:
                clean = namekey[~namekey.iata_code.isnull()][['location','iata_code']]
                clean.to_csv("app/static/clean.csv", index=False)
                return redirect(url_for("mileage_calculator.retry"))
            
        flash(error)

    return render_template('mileage_calculator/start.html')

@bp.route('/retry', methods=('GET', 'POST'))
def retry():
    #as a test, if it sucessfully retrievs the namekey redirect immediatley to the finish page (downloading the namekey)

    #do some transform to the sent argument then render the template
    #prevent viewing without earlier steps 
    if request.method == 'POST':

        completed_csv = request.files['corrected']
        completed_csv.save("app/static/completed.csv")
        completed = pd.read_csv("app/static/completed.csv")
        previously_completed = pd.read_csv("app/static/clean.csv")
        csv = pd.read_csv(session["csv_name"]) #fic this

        error = None

        if not completed_csv:
            error = "You must upload a file first"
        elif not set(['location']).issubset(completed.columns):
            error = 'location column missing, please leave columns intact'
        elif not set(['iata_code']).issubset(completed.columns):
            error = 'iata_code column missing, please leave columns intact'
        elif (completed['iata_code'].isna().any()):
            error = 'Please fill out all Iata codes and resubmit'

        #need to enforce that all given IATA codes are in fact valid 
        
        if error is None:

            completed = completed[['location','iata_code']]
            previously_completed = previously_completed[['location','iata_code']]
            namekey = pd.concat([completed, previously_completed])
            csv_clean = clean_spreadsheet(csv, namekey, session["origination_col"], session["destination_col"])
            return_df = calculate_distances(csv_clean, True)

            return_df = return_df.drop(['origination_code','orig_lat','orig_long','destination code','dest_lat','dest_long'], axis=1)
            return_df.to_csv("app/static/final_data.csv")

            return redirect(url_for("mileage_calculator.finish"))
        
        flash(error)

    return render_template('mileage_calculator/retry.html')


@bp.route('/finish', methods=('GET', 'POST'))
def finish():
    #origination_code,orig_lat,orig_long,destination code,dest_lat,dest_long
    return render_template('mileage_calculator/finish.html')

#Helpers below this point

def get_names(df, orig_col, dest_col):

    origins = df[orig_col]
    destinations = df[dest_col]
    locations = (pd.concat([origins, destinations])).drop_duplicates().reset_index(drop=True)
    names = pd.DataFrame(locations, columns = ['location'])

    #currently no effect, just duplicate
    names['alias'] = names['location'].apply(normalize)
    return names

def normalize(string):

    #TODO: strip whitespace, commas, underscores, make all lowercase 
    if(isinstance(string, str)):

        #strip unwanted characters 
        for value in [" ", ",", "_", "-"]:
            string = string.replace(value, "")
        
        string = string.replace("United States of America", "United States")
    return string


def clean_spreadsheet(df, namekey, orig_col, dest_col):

    aliases = pd.read_csv("aliases_csv.csv")
    new = df.merge(namekey, left_on=orig_col, right_on="location", how="left")
    new = new.merge(aliases, left_on="iata_code", right_on="iata_code", how="left")
    new = new.rename(columns={"iata_code":"origination_code", "latitude": "orig_lat", "longitude":"orig_long"})

    new = new.merge(namekey, left_on=dest_col, right_on="location", how="left")
    new = new.merge(aliases, left_on="iata_code", right_on="iata_code", how="left")
    new = new.rename(columns={"iata_code":"destination code", "latitude": "dest_lat", "longitude":"dest_long"})
    
    new = new.drop(['location_x', 'alias_x', 'location_y', 'alias_y'], axis=1)

    #df = new_df.drop(['location_x','location_y'], axis=1)
    new.to_csv("app/static/cleaned.csv")
    return new

def calculate_distances(df_in, miles):

    df = df_in

    if(miles):
        colname ='one_way_distance(miles)'
    else:
        colname = 'one_way_distance(km)' 

    df[colname] = np.nan

    for i in df.index:

        dist = haversine(df['orig_lat'][i], df['dest_lat'][i], df['orig_long'][i], df['dest_long'][i])

        if(miles):
            dist = dist / 0.6213711

        df[colname][i] = dist
    
    #df = new_df.drop(['location_x','location_y'], axis=1)
    return df

"""
NOTE: currently returns distance IN KM given the geocoordinates of two locations 
"""
def haversine(lat1, lat2, long1, long2):

    lat1, lat2, long1, long2 = map(radians, [lat1, lat2, long1, long2])

    lats = lat2 - lat1
    longs = long2 - long1
    a = sin(lats/2)**2 + cos(lat1) * cos(lat2) * sin(longs/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # radius of the earth
    return c * r 