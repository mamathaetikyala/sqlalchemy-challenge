# Flask API for hawaii stations - precipitations and temperature
# Import the dependencies. 
import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
measurement = Base.classes.measurement
station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Find the most recent date in the data set.
    most_recent_rec = session.query(measurement.date).order_by(measurement.date.desc()).first()
    
    # Design a query to retrieve the last 12 months of precipitation data and plot the results. 
    # Starting from the most recent data point in the database. 
    most_recent_date = most_recent_rec.date

    # Calculate the date one year from the last date in data set.
    date_one_year_back = (dt.datetime.strptime(most_recent_date, '%Y-%m-%d') - pd.offsets.DateOffset(months=12)).strftime("%Y-%m-%d")

    # Perform a query to retrieve the data and precipitation scores
    last_year_prcp = session.query(measurement.date ,measurement.prcp).filter(measurement.date >= date_one_year_back).all()

    session.close()
    
    #Build result data for API
    prcp_result = []
    for date, prcp in last_year_prcp:
        prcp_dict = {}
        prcp_dict[date] = prcp
        prcp_result.append(prcp_dict)
    
    return jsonify(prcp_result)


@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all stations """
    # Query all stations
    station_results = session.query(station.station).all()

    session.close()

    # Convert list of tuples into normal list
    all_stations = list(np.ravel(station_results))

    return jsonify(all_stations)


@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Find the most recent date in the data set.
    most_recent_rec = session.query(measurement.date).order_by(measurement.date.desc()).first()
    
    # Design a query to retrieve the last 12 months of precipitation data and plot the results. 
    # Starting from the most recent data point in the database. 
    most_recent_date = most_recent_rec.date

    # Calculate the date one year from the last date in data set.
    date_one_year_back = (dt.datetime.strptime(most_recent_date, '%Y-%m-%d') - pd.offsets.DateOffset(months=12)).strftime("%Y-%m-%d")

    # Design a query to find the most active stations (i.e. which stations have the most rows?)
    most_active_stations = session.query( measurement.station.label('station'), func.count(measurement.station).label('count') ).\
                                  group_by(measurement.station).\
                                  order_by(func.count().desc())
    
    # Using the most active station id from the previous query, date and temperature(tobs).
    sub = most_active_stations.limit(1).subquery()
    query = session.query(measurement.date, measurement.tobs).\
                     filter(measurement.station == sub.c.station).\
                     filter(measurement.date >= date_one_year_back)    

    most_active_station_tobs = query.all()
    
    
    session.close()
    
    #Build result data for API
    tobs_result = []
    for date, tobs in most_active_station_tobs:
        tobs_dict = {}
        tobs_dict[date] = tobs
        tobs_result.append(tobs_dict)
    
    return jsonify(tobs_result)

@app.route("/api/v1.0/<start>")
def tobs_agg_start(start):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Convert Input start to date format 
    from_date = dt.datetime.strptime(start, '%Y-%m-%d')

    # Using the start date get Min,Max and Average of temperature(tobs).
    query = session.query(func.min(measurement.tobs).label('TMIN'), func.max(measurement.tobs).label('TMAX') , func.avg(measurement.tobs).label('TAVG')).\
                     filter(measurement.date >= from_date)    

    aggregated_tobs = query.all()
    
    #Close db session
    session.close()
    
    #Build result data for API
    tobs__agg_result = []
    for TMIN, TMAX, TAVG in aggregated_tobs:
        tobs_agg_dict = {}
        tobs_agg_dict['TMIN'] = TMIN
        tobs_agg_dict['TAVG'] = TAVG
        tobs_agg_dict['TMAX'] = TMAX
        tobs__agg_result.append(tobs_agg_dict)
    
    return jsonify(tobs__agg_result)

@app.route("/api/v1.0/<start>/<end>")
def tobs_agg_start_end(start,end):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Convert Input start,end to date format 
    from_date = dt.datetime.strptime(start, '%Y-%m-%d')
    to_date = dt.datetime.strptime(end, '%Y-%m-%d')

    # Using the start date, end date get Min,Max and Average of temperature(tobs).
    query = session.query(func.min(measurement.tobs).label('TMIN'), func.max(measurement.tobs).label('TMAX') , func.avg(measurement.tobs).label('TAVG')).\
                     filter(measurement.date >= from_date).\
                     filter(measurement.date < to_date)    

    aggregated_tobs = query.all()
    
    #Close db session
    session.close()
    
    #Build result data for API
    tobs__agg_result = []
    for TMIN, TMAX, TAVG in aggregated_tobs:
        tobs_agg_dict = {}
        tobs_agg_dict['TMIN'] = TMIN
        tobs_agg_dict['TAVG'] = TAVG
        tobs_agg_dict['TMAX'] = TMAX
        tobs__agg_result.append(tobs_agg_dict)
    
    return jsonify(tobs__agg_result)

if __name__ == '__main__':
    app.run(debug=True)


