import pandas as pd
import numpy as np
import subprocess


from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)

from app.db import get_db

#need the 3rd argument or no?
bp = Blueprint('database_manager', __name__)

@bp.route('/managedb', methods=('GET', 'POST'))

def managedb():
    if request.method == "POST":
        alias_change_csv = request.files['alias_change']
        codes_change_csv = request.files['geocoordinate_change']

        error = None

        if not (alias_change_csv) and not (codes_change_csv):
            error = "No files submitted, please submit at least one file to continue"


        if(alias_change_csv):
            alias_change_csv.save("app/static/most_recent_aliases.csv")
            alias_change = pd.read_csv("app/static/most_recent_aliases.csv")
            if error == None and not set(['alias', 'iata_code']).issubset(alias_change.columns):
                error = "alias csv not in the correct format"
            else:
                alias_change.to_csv("app/static/most_recent_aliases.csv", index=False)
        else:
            alias_change = pd.read_csv("app/static/most_recent_aliases.csv")

        if(codes_change_csv):
            codes_change_csv.save("app/static/most_recent_codes.csv")
            codes_change = pd.read_csv("app/static/most_recent_codes.csv")
            if error == None and not set(['iata_code', 'latitude', 'longitude']).issubset(codes_change.columns):
                error = "codes csv not in the correct format"
            else:
                codes_change.to_csv("app/static/most_recent_codes.csv", index=False)

        if error == None:

            db = get_db()

            if(codes_change_csv):

                try:
                    db.execute('DELETE FROM codes')
                    
                    for i in codes_change.index:
                        db.execute("INSERT INTO codes (iata_code, latitude, longitude) VALUES(?, ?, ?)",
                            (codes_change['iata_code'][i], codes_change['latitude'][i], codes_change['longitude'][i]),) 
                    db.commit()

                except db.IntegrityError:
                    error = "unspecified error with code change"

            try:
                db.execute('DELETE FROM aliases')
                for i in alias_change.index:
                    db.execute("INSERT INTO aliases (alias, iata_code) VALUES(?, ?)",
                        (alias_change['alias'][i], alias_change['iata_code'][i]),) 
                db.commit()
                
            except db.IntegrityError:
                error = "unspecified error with alias change"
            
        
            return redirect(url_for("database_manager.checkresults"))
        
        flash(error)
        
    return render_template('database_manager/managedb.html')

@bp.route('/checkresults', methods=('GET', 'POST'))
def checkresults():

    error = None
    db = get_db()
    try:
        aliasrows = db.execute('SELECT COUNT(*) FROM aliases').fetchone()[0]
        codesrows = db.execute('SELECT COUNT(*) FROM codes').fetchone()[0]
    except:
        error = "something went wrong..."

    if(error):
        flash(error)
    
    return render_template('database_manager/checkresults.html', aliasrows=aliasrows, codesrows=codesrows)

