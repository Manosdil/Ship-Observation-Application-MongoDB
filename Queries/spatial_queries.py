from pymongo import MongoClient
import random
import plotly.express as px
import time

#connect with db
client = MongoClient("localhost", 27117)
db = client.maritime
collection = db.nari

def range_circle():

    coordinates = input('Set the longtitude and latitude and seperate them with comma: ')
    #transform coordinates to a list of floats
    coordinates = coordinates.split(",")
    coordinates = [float(x) for x in coordinates]
    #getting the max distance
    max_distance = int(input('Type the maximun distance: '))
    #construction of query
    tic = time.time()
    result = collection.aggregate([{'$geoNear': {'near': {'type': 'Point', 'coordinates': [coordinates[0],coordinates[1]]},
                            'distanceField': 'dist.calculated', 'maxDistance': max_distance,
                             'includeLocs': 'dist.location', 'spherical': True, 'key': 'spatiotemp.location'}}, {
                                '$unwind': {'path': '$spatiotemp', 'preserveNullAndEmptyArrays': True}}, {
                                '$match': {'spatiotemp.location': { '$geoWithin': {
                                     '$centerSphere': [[coordinates[0],coordinates[1]], max_distance/(6378.1 * 1000)]}}}}])
    print(f'Time of query: {time.time()-tic}')
    #initialize lists to keep same color for each mmsi
    mmsi_list = []
    color_list = []
    #initialize a dictionary to add data for plotting trajectories
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

def knn():
    #setting the coordinates
    coordinates = input('Set the longtitude and latitude and seperate them with comma: ')
    coordinates = coordinates.split(",")
    coordinates = [float(x) for x in coordinates]
    print(coordinates)
    #getting the max distance
    max_distance = int(input('Type the maximun distance: '))
    k = int(input('How may neighboors?: '))

    tic = time.time()
    result = collection.aggregate([{'$geoNear': {'near': {'type': 'Point', 'coordinates': [coordinates[0],coordinates[1]]},
        'distanceField': 'dist.calculated', 'maxDistance': max_distance, 'includeLocs': 'dist.location', 'spherical': True, 'key': 'spatiotemp.location'}}
                , {'$limit':k+15}])
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

