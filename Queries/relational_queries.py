from pymongo import MongoClient
import random
import plotly.express as px
import time


#connect with database and use the right collection
client = MongoClient("localhost", 27117)
db = client.maritime
collection = db.nari

def relational_query():
    #asking what kind of relation the user wants to check
    relation = input("What kind of Relational Query: (if more than one seperate them with comma)\
                    \n1) Flag (type: shipflag)\
                    \n2) ShipName (type: shipname)\
                    \n3) Shiptype(type: shiptype)\
                    \n4) Navigational Status (type: nav_status)\n") 

    #creating a list which the features that
    #user selected are included
    relations = relation.split(",")
    relations = [x.strip() for x in relations if x!=" " and x!=""]
    
    #creating a list of lists
    #each element of a list are the values that 
    #the user chose for each feature
    query_list = []
    for feature in relations:
        feature = feature.capitalize()
        query = input('Enter the ' + feature + '\s you want to find.\n')
        query = query.split(",")
        query = [x.strip() for x in query if x!=" " and x!=""]
        query_list.append(query)

    #getting data from database 
    if len(query_list) == 1 :
        tic = time.time()
        result = collection.find({relations[0]:{"$in": query_list[0]}})
        print(f'Time of query: {time.time()-tic}')
    elif len(query_list) == 2 :
        tic = time.time()
        result = collection.find({relations[0]:{"$in": query_list[0]}, relations[1]:{"$in": query_list[1]}})
        print(f'Time of query: {time.time()-tic}')
    elif len(query_list) == 3 :
        tic = time.time()
        result = collection.find({relations[0]:{"$in": query_list[0]},
        relations[1]:{"$in": query_list[1]}, relations[2]:{"$in": query_list[2]}})
        print(f'Time of query: {time.time()-tic}')
    else : 
        tic = time.time()   
        result = collection.find({relations[0]:{"$in": query_list[0]},
        relations[1]:{"$in": query_list[1]}, relations[2]:{"$in": query_list[2]}
        , relations[3]:{"$in": query_list[3]}})
        print(f'Time of query: {time.time()-tic}')
    #initialize two lists one to keep the sourcemmsi
    #and another one to keep the color that used for the specific ship
    #in order to use the same color for the ship in map 
    #if the specific ship exists in more than two documents
    mmsi_list = []
    color_list = []
    #initialize a dictionary in which we will add data for plotting trajecories
    dictionary = {'mmsi':[], 'lat': [] , 'lon': [] , 'timestamp': []}
    #itterate over the documents
    #we got from collection
    for doc in result: 
        #add a specific color to each sourcemmsi randomly
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
        for loc in doc['spatiotemp']:
            dictionary['mmsi'].append(doc['_id']['sourcemmsi'])
            dictionary['lat'].append(loc['location']['coordinates'][1])
            dictionary['lon'].append(loc['location']['coordinates'][0])
            dictionary['timestamp'].append(loc['timestamp'])
        doc.pop('_id')
    print(f'Time of app to process data: {time.time()-tic}')
    return dictionary
    
#specific sourcemmsi
def sourcemmsi():
    mmsi = input('Type the mmsi: ')

    tic = time.time()
    result = collection.find({'_id.sourcemmsi':mmsi})
    print(f'Time of query: {time.time()-tic}')

    dictionary = {'mmsi':[], 'lat': [] , 'lon': [] , 'timestamp': []}
    for doc in result:
        for loc in doc['spatiotemp']:
            dictionary['mmsi'].append(doc['_id']['sourcemmsi'])
            dictionary['lat'].append(loc['location']['coordinates'][1])
            dictionary['lon'].append(loc['location']['coordinates'][0])
            dictionary['timestamp'].append(loc['timestamp'])

    print(f'Time of app to process data: {time.time()-tic}')
    return dictionary