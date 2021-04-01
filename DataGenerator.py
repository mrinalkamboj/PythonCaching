import random
import time
from Caching import GetCacheKey,GetCacheData,SetCacheData

'''
===========================================================================================
'''

totalRecords = 1000000

def generateName():
    namePrefixes = ["M","N","Z","Y","X"]    
    nameIds = range(1,10)
    name = random.choice(namePrefixes) + str(random.choice(nameIds))
    return name

def generateId():       
    ids = range(1,20)
    return random.choice(ids)

def generateLevel():       
    levels = ["Architect","Developer","Sr Developer","Manager","Director","Ceo"]
    return random.choice(levels)

def generateTraits():
    traits = ["excellent","good","fair"]
    return random.choices(traits,k=2)

'''
===========================================================================================
'''

'''
Generate the Cache Key (Using Selective Data)
'''
def generateData(primaryKeyDictionary:dict,cache_key:str):
    
    print("Generate or Fetch Cached Data")

    start = time.time() # time profiling
    
    key = GetCacheKey(primaryKeyDictionary,cache_key)    
    
    cacheData = GetCacheData(key)
    
    if cacheData is not None:
        print("Data Generation Time (Cached) % s seconds" % (time.time() - start))
        return cacheData
    else:
        cacheData = []
    
    gdCounter = 1
    
    while gdCounter <= 1000000:
        d = dict()
        d["id"] = generateId()
        d["name"] = generateName()
        d["level"] = generateLevel()
        d["traits"] = generateTraits()
        cacheData.append(d)
        gdCounter = gdCounter + 1
    
    SetCacheData(key,cacheData)

    print("Data Generation Time (DB) % s seconds" % (time.time() - start))
    return cacheData
    
    
    
    