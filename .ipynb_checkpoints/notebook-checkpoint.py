#!/usr/bin/env python
# coding: utf-8

# #### LIBRARIES

# In[1]:


import requests
import pandas as pd
import numpy as np
import requests
import json
import pytz
import requests
import io
from dotenv import load_dotenv
import os
import tweepy
import emoji
import schedule
import time


# In[2]:


load_dotenv()


# #### GET DATA FROM GCBA BIKESHARING API

# In[ ]:


############## Station Status ####################

load_dotenv()

def get_data_and_post ():
# API endpoint
    url = os.getenv('stations_status_url')

    # Client ID and Client Secret
    client_id = os.getenv('GCBA_CLIENT_ID')
    client_secret = os.getenv('GCBA_CLIENT_SECRET')

    # Query parameters
    params = {
        "client_id": client_id,
        "client_secret": client_secret,
        "json": 1
    }

    def call_transport_api(url, params):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()  # Raise an exception for bad status codes
            data = response.json()
            return data
        except requests.exceptions.RequestException as e:
            print("Error:", e)
            print("Response content:", response.content)  # Print response content for debugging
            return None

    # Call the API
    api_status_data = call_transport_api(url, params)

    # Check if data is retrieved successfully
    if api_status_data:
        # Save API data to a file
            with open("api_status_data.json", "w") as f:
                json.dump(api_status_data, f)
            
    # Transformations
    
    # Flatten the nested JSON data
    stations_data = api_status_data['data']['stations']
    df_status = pd.json_normalize(stations_data)


    # Create a new column with the value of 'last_updated'
    df_status['last_updated'] = api_status_data['last_updated']

    columns_to_convert = ['last_updated','last_reported']
    for column in columns_to_convert:
        df_status[column] = pd.to_datetime(df_status[column], unit='s')

    # Convert UTC time to Argentina time zone
    argentina_tz = pytz.timezone('America/Argentina/Buenos_Aires')
    for column in columns_to_convert:
        df_status[column] = df_status[column].dt.tz_localize(pytz.utc).dt.tz_convert(argentina_tz)
        
    # Create variables with metrics
    
    # Total stations in service
    in_service_station = df_status[df_status['status'] == 'IN_SERVICE'].count()[0]

    # Number of in service station with bikes available and with more than one bike available
    bikes_available = df_status[(df_status['num_bikes_available'] > 0) & (df_status['status'] == 'IN_SERVICE')].count()[0]
    bikes_available_1 = df_status[(df_status['num_bikes_available'] > 1) & (df_status['status'] == 'IN_SERVICE')].count()[0]


    # Percentage with bikes available
    bikes_available_perc = round((bikes_available / in_service_station)*100,2)
    bikes_available_1_perc = round((bikes_available_1 / in_service_station)*100,2)

    # Number of available and disabled bikes
    bikes_available_in_stations = df_status['num_bikes_available'].sum()
    bikes_disabled = df_status['num_bikes_disabled'].sum()

    # Average for available bikes in stations
    bikes_available_in_stations_avg = round(df_status['num_bikes_available'].mean(),2)
    bikes_disabled_in_stations_avg = round(df_status['num_bikes_disabled'].mean(),2)
    
    #### POST
    
    #### Get the keys to log

    ACCESS_KEY = os.getenv('x_access_key')
    ACCESS_SECRET = os.getenv('x_access_secret')
    CONSUMER_KEY = os.getenv('x_consumer_key')
    CONSUMER_SECRET = os.getenv('x_consumer_secret')
    BEARER_TOKEN = os.getenv('x_bearer_token')
    ACCESS_TOKEN = os.getenv('x_access_token')
    ACCESS_TOKEN_SECRET = os.getenv('x_access_token_secret')
    
    
    # Authenticate to Twitter
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(
        ACCESS_TOKEN,
        ACCESS_TOKEN_SECRET,
    )
    
    # this is the syntax for twitter API 2.0. It uses the client credentials that we created
    newapi = tweepy.Client(
        bearer_token=BEARER_TOKEN,
        access_token=ACCESS_TOKEN,
        access_token_secret=ACCESS_TOKEN_SECRET,
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET
    )

    api = tweepy.API(auth)
    
    #### Tweet 1
    
    estaciones_post = newapi.create_tweet(text=
                                  "📍Estaciones:\n \n" \
                                  "· 👍 En servicio -> " + in_service_station.astype('str') +
                                  "\n· ✅ Con bicicletas disponibles -> " + bikes_available.astype('str')+ " | " + bikes_available_perc.astype('str') + "%"
                                  "\n· ✅ ✅ Con más de una bicicleta disponible -> " + bikes_available_1.astype('str') + " | " + bikes_available_1_perc.astype('str') + "%"
                                 )
    #### Tweet 2
    
    bicicletas_post = newapi.create_tweet(text=
                                  "🚲Bicicletas:\n \n" \
                                  "\n· ✅ Total Disponibles en estación -> " + bikes_available_in_stations.astype('str') +
                                  "\n· 🛑 Total Deshabilitadas -> " + bikes_disabled.astype('str') +
                                  "\n· ✅ Promedio disponible por estación -> " + str(bikes_available_in_stations_avg) +
                                  "\n· 🛑 Promedio deshabilitadas por estación -> " + str(bikes_disabled_in_stations_avg)
                                  
                                 )
    
# Schedule the task to run every 2 hours
schedule.every(2).minutes.do(get_data_and_post)

# Run the scheduler
while True:
    schedule.run_pending()
    time.sleep(5)
    


# In[ ]:




