from pymongo import MongoClient
import plotly.express as px
from tslearn.metrics import dtw
from collections import defaultdict
from datetime import datetime
import time


#connect with database 
client = MongoClient("localhost", 27117)
db = client.maritime
collection = db.nari

def k_similar_traj():
    mmsi = input('Type the sourcemmsi: ')
    n_sim = int(input('K-similar: '))
    date0 = input('Set the first date in the following format: (dd/mm/yy HH:MM:SS)')
    date0 = datetime.strptime(date0, "%d/%m/%y %H:%M:%S").timestamp()
    date1 = input('Set the second date in the following format: (dd/mm/yy hh:mm:ss)')
    date1 =  datetime.strptime(date1, '%d/%m/%y %H:%M:%S').timestamp()
    tic = time.time()
    result = collection.aggregate([{'$match': {'_id.sourcemmsi': mmsi,
                        'spatiotemp.timestamp': {'$gte': date0, '$lte': date1}}}, 
                        {'$unwind': {'path': '$spatiotemp', 'preserveNullAndEmptyArrays': True}}, 
                        {'$match': {'spatiotemp.timestamp': {'$gte': date0,'$lte': date1}}}, 
                        {'$sort': {'spatiotemp.timestamp': 1}}])


    result2 = collection.aggregate([{'$match': {'_id.sourcemmsi':{'$ne':mmsi},
                        'spatiotemp.timestamp': {'$gte': date0, '$lte': date1}}}, 
                        {'$unwind': {'path': '$spatiotemp', 'preserveNullAndEmptyArrays': True}}, 
                        {'$match': {'spatiotemp.timestamp': {'$gte': date0,'$lte': date1}}}, 
                        {'$sort': {'spatiotemp.timestamp': 1}}])
    print(f'Time of query: {time.time()-tic}')

    trajectory = []
    for doc in result:
        trajectory.append(doc['spatiotemp']['location']['coordinates'])

    mmsis= defaultdict(list) 
    for doc in result2:
        mmsis[doc['_id']['sourcemmsi']].append(doc['spatiotemp']['location']['coordinates'])

    similarity = defaultdict(float)
    list_to_sort = []
    for k, v in mmsis.items():
        list_to_sort.append([k,dtw(v, trajectory)])
    list_sorted = sorted(list_to_sort, key = lambda x:x[1])
    
    i=0
    dict_print = {'mmsi':[], 'lat':[], 'lon':[]}
    for ship in list_sorted:
        #flag for k-similar
        if i == n_sim:
            break
        #threshold. if bigger than 25km then it's not so simial
        if ship[1]>25:
            break

        if len(mmsis[ship[0]])<80:
            continue
        for coor in mmsis[ship[0]]:
            dict_print['mmsi'].append(ship[0])
            dict_print['lat'].append(coor[1])
            dict_print['lon'].append(coor[0])
        i+=1
    
    for t in trajectory:
        dict_print['mmsi'].append(mmsi)
        dict_print['lat'].append(t[1])
        dict_print['lon'].append(t[0])

    print(f'Time of app: {time.time()-tic}')
    
    return dict_print