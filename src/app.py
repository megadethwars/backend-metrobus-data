from flask import Flask, jsonify, request, Response
from flask_pymongo import PyMongo
from bson import json_util
from bson.objectid import ObjectId
import json
import threading
import traceback
import pymongo
from pymongo import MongoClient
from CsvLoader import CSV_LOADER


app = Flask(__name__)

app.secret_key = '1231kdlfkgdfgk45345'

client = MongoClient(host='test_mongodb',port=27017)

db = client['Metrobus']


# class csv loader
loader = CSV_LOADER()

#run schedule to update periodiclly data from url metrobus
loader.thread_run_schedule(db)



# http states
def bad_request(message):
    response = jsonify({'message': message})
    response.status_code = 400
    return response

def not_found_url(message):
    response = jsonify({'message': message})
    response.status_code = 404
    return response

def int_server(message):
    response = jsonify({'message': message})
    response.status_code = 500
    return response

def ok_server_post(message='ok'):
    response = jsonify({'message': message})
    response.status_code = 201
    return response


def ok_server_put(message='ok'):
    response = jsonify({'message': message})
    response.status_code = 200
    return response

def unauthorized(message = 'unauthorized'):
    response = jsonify({'message': message})
    response.status_code = 401
    return response

def conflict(message = 'conflict'):
    response = jsonify({'message': message})
    response.status_code = 409
    return response




@app.route('/', methods=['GET'])
def home():
    return 'OK'

# endpoint alcalias
@app.route('/alcaldias',methods=['GET'])
def get_alcaldias():
    try:
        objectDelegations = db.stations.distinct("delegation")

        if objectDelegations is None:
            return not_found_url('not found')

        new_dict ={}
        count=0
        for delegation in objectDelegations:
            if delegation!="":

                if str(delegation)!="no del":

                    count=count+1
                    new_dict.update({str(count): str(delegation)})
                


        jsonDelegations =  json.dumps(new_dict)


        return jsonDelegations,200, {'ContentType':'application/json'}
    except Exception as e:
        print(e)
        return int_server('Error server')

#endpoint unidades, get all available units 
@app.route('/unidades',methods=['GET'])
def get_units():
    try:
        objectunits= db.events.distinct('vehicle_id')
        if objectunits is None:
            return not_found_url('not found')

        list_vehicles=[]
        for vehicleid in objectunits:
            objectvehicle = db.events.find_one({ 'vehicle_id' : vehicleid })
            objectvehicle.pop('_id')
            objectvehicle.pop('idevent')
            objectvehicle.pop('date_updated')
            objectvehicle.pop('lat')
            objectvehicle.pop('lon')
            objectvehicle.pop('speed')
            objectvehicle.pop('odometer')
            objectvehicle.pop('trip_route_id')
            objectvehicle.pop('delegacion')
            
            list_vehicles.append(objectvehicle)
        
        jsonList_vehicles = json.dumps(list_vehicles)


        return jsonList_vehicles,200, {'ContentType':'application/json'}
    except:
        return int_server('Error server')

#get all available stations
@app.route('/AllStations',methods=['GET'])
def get_allStations():
    try:
        available_stations = db.stations.distinct('nombre')
        
        if available_stations is None:
            return not_found_url('not found')


        list_stations=[]
        for stationname in available_stations:
            objectstation = db.stations.find_one({ 'nombre' : stationname })
            objectstation.pop('_id')           
            objectstation.pop('wk_geom')
            
            
            
            list_stations.append(objectstation)
        
        jsonList_stations = json.dumps(list_stations)
        
        return jsonList_stations,200, {'ContentType':'application/json'}
    except Exception as e:
        print(e)
        return int_server('Error server')

# get only units by alcaldia,write a known delegation as parameter
@app.route('/unitsByAlcaldia/<name>',methods=['GET'])
def get_unitsPlace(name):
    try:
        eventsbyalcaldia = db.events.find({'delegacion':name})

        count=0
        list_events=[]
        for events in eventsbyalcaldia:
            count=count+1
            list_events.append(events)

        

        if count==0:
            jsonempty={}
            return json.dumps(jsonempty),200, {'ContentType':'application/json'}

        response = json_util.dumps(list_events)
        return Response(response, mimetype="application/json")


    except:
        return int_server('Error server')

#get all movements made by unit, write the id of vehicle 
@app.route('/historialmovimientos/<id>',methods=['GET'])
def get_historialMovements(id):
    try:
        id_vehicle=0
        try:
            id_vehicle = int(id)
        except:
            return bad_request('bad request, not integer')

        objecthistorial = db.events.find({ 'vehicle_id' : id_vehicle })
        
        
        count=0
        list_historial=[]
        for historial in objecthistorial:
            count=count+1
            list_historial.append(historial)

        

        if count==0:
            jsonempty={}
            return json.dumps(jsonempty),200, {'ContentType':'application/json'}

        

        response = json_util.dumps(list_historial)
        return Response(response, mimetype="application/json")
    except Exception as e:
        print(e)
        return int_server('Error server')

if __name__ == "__main__":
    app.run(debug=True,host = '0.0.0.0')