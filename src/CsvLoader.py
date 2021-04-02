import pandas as pd
from geopy.geocoders import GoogleV3,Nominatim
import threading
import traceback
import json
import numpy as np
import requests
import io
import pymongo 
import schedule
import time
import math
import signal
import sys
from random import seed
from random import random

delegations = ['Azcapotzalco','Gustavo A. Madero','Cuauhtémoc','Venustiano Carranza','Benito Juárez','Iztapalapa','Iztacalco','Miguel Hidalgo','Álvaro Obregón','Coyoacán','Tlalpan','Milpa Alta','Xochimilco','Tláhuac','Magdalena COntreras','Cuajimalpa','']
seed(1)
# url to download
url_stations="https://datos.cdmx.gob.mx/dataset/86d42b17-c34d-45be-b14b-6af615962a00/resource/092cfcab-4a2e-44af-86c3-eede7f7a6c80/download/estaciones-metrobus.csv"
url_events = "https://datos.cdmx.gob.mx/dataset/32b08754-ae92-4fbd-86d3-261cc64b6ca8/resource/ad360a0e-b42f-482c-af12-1fd72140032e/download/prueba_fetchdata_metrobus.csv"
class CSV_LOADER():

    def __init__(self):
        self.isFinished=False
        

    def correct_encoding(self,dictionary):
        """Correct the encoding of python dictionaries so they can be encoded to mongodb
        inputs"""

        new = {}
        for key1, val1 in dictionary.items():
         

            if isinstance(val1, np.bool_):
                val1 = bool(val1)

            if isinstance(val1, np.int64):
                val1 = int(val1)

            if isinstance(val1, np.float64):
                val1 = float(val1)

            new[key1] = val1

        return new
    
    #funciton to load event units
    def Load_events(self,db):
        row=None
        try:
            print('init sync events')
            req=None
            df_events=None

            isDownloaded=False
            for x in range(10):
                try:
                    #download csv from url
                    req=requests.get(url_events).content
                    df_events=pd.read_csv(io.StringIO(req.decode('utf-8')))
                    df_events.fillna(0)
                    #replace nan fiald by 0
                    df_events.replace(np.nan, 0)
                    isDownloaded=True
                    break
                except:
                    print("error download csv")
            
            if isDownloaded == False:
                return
            print('downloaded') 
            r, c = df_events.shape
            
            
            geolocalizador = Nominatim(user_agent="test-py-0"+str(random()))
            for x in range(r):

                try:

                    if self.isFinished==True:
                        break

                    #check delegation based in given coordenates
                    row = df_events.iloc[x]
                    coordenates = str(df_events.iloc[x]['position_latitude'])+','+str(df_events.iloc[x]['position_longitude'])
                    ubicacion = geolocalizador.reverse(coordenates)

                    fields=None
                    current_delegation="No del"
                    try:
                        fields =str(ubicacion[0]).split(',')
                        for field in fields:
                            if field[0]==' ':
                                field=field[1:]
                            for delegation in delegations:
                                delegation=str(delegation)

                                if field == delegation:
                                    
                                    current_delegation=field
                                    break
                    except Exception as e:
                        print(e)
                    
                    
                    #fill dictionary by row csv object
                    event_dict = {'idevent': df_events.iloc[x]['id'],                          
                                    'date_updated': df_events.iloc[x]['date_updated'], 
                                    'vehicle_id': df_events.iloc[x]['vehicle_id'],
                                    'vehicle_label':df_events.iloc[x]['vehicle_label'],
                                    'vehicle_status':df_events.iloc[x]['vehicle_current_status'],
                                    'lat':df_events.iloc[x]['position_latitude'],
                                    'lon':df_events.iloc[x]['position_longitude'],
                                    'point':df_events.iloc[x]['geographic_point'],
                                    'speed':df_events.iloc[x]['position_speed'],
                                    'odometer':df_events.iloc[x]['position_odometer'],                                                    
                                    'trip_route_id':0.0 if np.isnan(np.min(row['trip_route_id'])) else df_events.iloc[x]['trip_route_id'],
                                    'delegacion':current_delegation                   
                                    }
                    #correct encoding to insert mongodb
                    new_dict =self.correct_encoding(event_dict)
                   
                    #inserting documents if not exist in database
                    existing_document = db.events.find_one(new_dict)
                    if not existing_document:
                        db.events.insert(new_dict)
                        print("inserting")
                    
                    

                  
                except Exception as e:
                    print(e)
                    
              

        except Exception as e:
            print(e)
            


    #function to load stations
    def LoadCSV_Stations(self,db):

        try:
            print('init sync stations')

          
            req=None
            df_stations=None

            isDownloaded=False
            for x in range(10):
                try:
                    #download csv statios from url
                    req=requests.get(url_stations).content
                    df_stations=pd.read_csv(io.StringIO(req.decode('utf-8')))
                    df_stations.fillna(0)
                    isDownloaded=True
                    break
                except:
                    print("error download csv")
            
            if isDownloaded == False:
                return

            print('downloaded')    
            r, c = df_stations.shape

            geolocalizador = Nominatim(user_agent="test-py-0"+str(random()))
            for x in range(r):

                if self.isFinished==True:
                    break
                #check delegation based in given coordenates
                coordenates = str(df_stations.iloc[x]['lat'])+','+str(df_stations.iloc[x]['lon'])
                ubicacion = geolocalizador.reverse(coordenates)

                fields=None
                current_delegation="no del"

                try:
                    fields =str(ubicacion[0]).split(',')
                    for field in fields:
                        if field[0]==' ':
                            field=field[1:]
                        for delegation in delegations:
                            delegation=str(delegation)

                            if field == delegation:
                                
                                current_delegation=field
                                break
                except Exception as e:
                    print(e)

                
                #fill dictionary by row csv object
                station_dict = {'idestacion': df_stations.iloc[x]['id'],
                                'wk_geom': df_stations.iloc[x]['wkt_geom'], 
                                'nombre': df_stations.iloc[x]['nombre'],
                                'linea':df_stations.iloc[x]['linea'],
                                'lat':df_stations.iloc[x]['lat'],
                                'lon':df_stations.iloc[x]['lon'],
                                'delegation':current_delegation
                                }
                
                #correct encoding to inster into database
                new_dict = self.correct_encoding(station_dict)

                #insert documento to db if nos exist
                existing_document = db.stations.find_one(new_dict)
                if not existing_document:
                    db.stations.insert(new_dict)
                    print("inserting")
                
            #call function to load events
            self.Load_events(db)

        except Exception as e:
            print(e)
           
    def run_schedule(self,db):

        try:
            self.Load_events(db)
            self.LoadCSV_Stations(db)
            #implement an schedule to run every 23 hours, for updating data into database
            schedule.every(23).hours.do(self.LoadCSV_Stations,db)
            
            while self.isFinished==False:
                schedule.run_pending()
                time.sleep(5)
        except Exception as e:
            print(e)
                 
        print('finishing')


    def signal_handler(self,sig, frame):
        print('You pressed Ctrl+C!')
        self.isFinished=True
        sys.exit(0)

    def thread_run_schedule(self,db):
        try:
            print('starting ...csv dowloader')
            #threading to star scheduler
            th = threading.Thread(target=self.run_schedule,args=(db,))
            th.start()
            
            signal.signal(signal.SIGINT, self.signal_handler)
            
            
        except Exception as e:
            print(e)

    



#loader = CSV_LOADER()

#print('starting ...')

#loader.run_schecule(db)










