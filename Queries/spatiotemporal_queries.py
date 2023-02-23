from pymongo import MongoClient
import random
import plotly.express as px
from datetime import datetime
import time 

#connect with database and use the right collection
client = MongoClient("localhost", 27117)
db = client.maritime
collection = db.nari

def spatiotemporal_range():
    #setting the coordinates
    date0 = input('Set the first date in the following format (dd/mm/yy HH:MM:SS): ')
    date0 = datetime.strptime(date0, "%d/%m/%y %H:%M:%S")
    date1 = input('Set the second date in the following format (dd/mm/yy hh:mm:ss): ')
    date1 =  datetime.strptime(date1, '%d/%m/%y %H:%M:%S')
    coordinates = input('Set the longtitude and latitude. Seperate them with comma:')
    coordinates = coordinates.split(",")
    coordinates = [float(x) for x in coordinates]
    #getting the max distance
    max_distance = int(input('Type the maximun distance: '))
    tic = time.time()
    result = collection.aggregate([{'$geoNear': {
        'near': {
        'type': 'Point',
        'coordinates': [coordinates[0],coordinates[1]]},
        'distanceField': 'dist.calculated',
        'maxDistance': max_distance,
        'includeLocs': 'dist.location',
        'spherical': True,
        'key': 'spatiotemp.location'}}
        , {'$match': {'spatiotemp.timestamp': {'$gte': date0.timestamp(), '$lte': date1.timestamp()}}}
        , {'$unwind': {'path': '$spatiotemp', 'preserveNullAndEmptyArrays': True}}
        , {'$match': {'spatiotemp.location': {'$geoWithin': {'$centerSphere': [[coordinates[0],coordinates[1]] 
        , max_distance/(6378.1 * 1000)]}}
        ,'spatiotemp.timestamp': {'$gte': date0.timestamp(), '$lte': date1.timestamp()}}}])
    print(f'Time of query: {time.time()-tic}')

    mmsi_list = []
    color_list = []
    dictionary = {'mmsi':[], 'lat': [] , 'lon': []}   

    for doc in result: 
        if doc['_id']['sourcemmsi'] not in mmsi_list:
            col = random.choice(['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'lightred'
                                    ,'beige', 'darkblue', 'darkgreen', 'cadetblue', 'darkpurple'
                                    , 'pink', 'lightblue', 'lightgreen'
                                    ,'gray', 'black', 'lightgray'])
            mmsi_list.append(doc['_id']['sourcemmsi'])
            color_list.append(col)
        else: 
            index = mmsi_list.index(doc['_id']['sourcemmsi'])
            col = color_list[index]
            #get all coordinates from document and add them to the map we initialized earlier
        
        dictionary['mmsi'].append(doc['_id']['sourcemmsi'])
        dictionary['lat'].append(doc['spatiotemp']['location']['coordinates'][1])
        dictionary['lon'].append(doc['spatiotemp']['location']['coordinates'][0])
            
        
    print(f'Time of app: {time.time()-tic}')
    return dictionary


def spatiotemp_knn():
    #setting the coordinates
    date0 = input('Set the first date in the following format(dd/MM/yyyy hh:mm:ss): ')
    date0 = datetime.strptime(date0, "%d/%m/%y %H:%M:%S")
    date1 = input('Set the second date in the following format(dd/MM/yyyy hh:mm:ss): ')
    date1 =  datetime.strptime(date1, '%d/%m/%y %H:%M:%S')
    coordinates = input('Set the longtitude and latitude and seperate them with comma: ')
    coordinates = coordinates.split(",")
    coordinates = [float(x) for x in coordinates]
    print(coordinates)
    #getting the max distance
    max_distance = int(input('Type the maximun distance: '))
    k = int(input('How may neighboors?'))
    tic = time.time()
    result = collection.aggregate([{'$geoNear': {'near': {'type': 'Point', 'coordinates': [coordinates[0],coordinates[1]]},
        'distanceField': 'dist.calculated', 'maxDistance': max_distance, 'includeLocs': 'dist.location', 'spherical': True, 'key': 'spatiotemp.location'}}
                , {"$match": {'spatiotemp.timestamp': {'$gte': date0.timestamp(), '$lte': date1.timestamp()}}},{'$limit':k+10}])
    print(f'Time of query: {time.time()-tic}')
    mmsi_list = []
    color_list = []
    dictionary = {'mmsi':[], 'lat': [] , 'lon': []}                
    i=0
    for doc in result: 
        if i ==k:
            break
        if doc['_id']['sourcemmsi'] not in mmsi_list:
            col = random.choice(['red', 'blue', 'green', 'purple', 'orange', 'darkred', 'lightred'
                                    ,'beige', 'darkblue', 'darkgreen', 'cadetblue', 'darkpurple'
                                    , 'pink', 'lightblue', 'lightgreen'
                                    ,'gray', 'black', 'lightgray'])
            mmsi_list.append(doc['_id']['sourcemmsi'])
            color_list.append(col)
        else: 
            continue
            #get all coordinates from document and add them to the map we initialized earlier
        
        dictionary['mmsi'].append(doc['_id']['sourcemmsi'])
        dictionary['lat'].append(doc['dist']['location']['coordinates'][1])
        dictionary['lon'].append(doc['dist']['location']['coordinates'][0])
        i+=1
        doc.pop('_id')
    print(f'Time of app: {time.time()-tic}')
    return dictionary
