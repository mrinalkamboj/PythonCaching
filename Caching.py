import enum
import json
import os
import pickle
import copy
from redis import Redis
from pickle import dumps,loads
from compress_pickle import dumps as compress_dumps
from compress_pickle import loads as compress_loads

from Helper.lists import SortKeyPick,PaginateResult
from CacheInit import cached_Data,cache_type_env,cache_lock
from CacheGetSize import get_size

import CacheConstants as Constants
import redis_lock
# from redis_lock import Lock as redislock

'''
Generate the Cache Key (Using Selective Data)
We utilize the primaryKeyFilter and cache_key
'''
def GetCacheKey(primaryKeyFilter:dict,
                cache_key = None):
    
    finalCacheKey = None # Final Cache Key fetched from Cache_Key and PK Parameters
    
    # # primaryKeyDict = kwargs["primaryKeyFilter"] # Fetch PK Dictionary
    
    # # cache_key = kwargs["cache_key"] # Fetch Cache Key
    
    if cache_key is not None: # For None Cache Key there will not be data caching
        
        cacheKeyDict = {}
        
        cacheKeyDict["cache_key"] = cache_key
        
        sortedPkDict = {}
        
        # Sort the PK by keys for Caching
        if primaryKeyFilter is not None:
            sortedPkDict = {k:v for k, v in sorted(primaryKeyFilter.items(), key=lambda item: item[0])}
        
        # Merge Dictionaries
        finalDict = {**cacheKeyDict,**sortedPkDict}
        
        # Json serialize and Cache Data
        # Explicitly make the Cache Key lower-case
        finalCacheKey = json.dumps(finalDict).lower()
    
    print("Cache-Key {finalCacheKey}".format(finalCacheKey=finalCacheKey))
    return finalCacheKey


'''
Get Data from the Cache both local and distributed
'''
def GetCacheData(key:str):   
    
    if cache_type_env == Constants.Cache_Type_Local: # Local Cache
        with cache_lock:
            result = None        
            if key in cached_Data:
                if Constants.IsPickled is True:
                    if Constants.IsCompressed:
                        print("Compressed Pickle")
                        result = compress_loads(cached_Data[key],compression=Constants.CompressionType)
                    else:
                        print("Only Pickle no Compression")
                        result = loads(cached_Data[key])
                else:
                    print("Un-Compressed")
                    result = []
                    for d in cached_Data[key]:
                        result.append(d.copy())
            return result
    elif cache_type_env == Constants.Cache_Type_Distributed: # Distributed Cache
        # rl = redis_lock.Lock(cached_Data,key)        
        # try:
        #     rl.acquire(blocking=True)
            # Get the Cached Data from the Distributed Cache
        with redis_lock.Lock(cached_Data,key):
            cachedResult = cached_Data.get(key)
            
            if cachedResult is not None:
                if Constants.IsPickled is True:
                    if Constants.IsCompressed:
                        return compress_loads(cachedResult,compression=Constants.CompressionType)
                    else:
                        return loads(cachedResult) # Cached data is Pickled binary serialization and stored
            else:
                return cachedResult
        # finally:
        #     rl.release()
    else: # Local Cache (Default)
        with cache_lock:
            result = None        
            if key in cached_Data:
                if Constants.IsPickled is True:
                    if Constants.IsCompressed:
                        print("Compressed Pickle")
                        result = compress_loads(cached_Data[key],compression=Constants.CompressionType)
                    else:
                        print("Only Pickle no Compression")
                        result = loads(cached_Data[key])
                else:
                    print("Un-Compressed")
                    result = []
                    for d in cached_Data[key]:
                        result.append(d.copy())
            return result

'''
Set Data to the Cache both local and distributed
'''
def SetCacheData(key:str,data):
        
        if Constants.MeasureSize is True:
            print("Original Data Size :: {}".format(get_size(data)))
        
        if cache_type_env == Constants.Cache_Type_Local: # Local Cache
            with cache_lock:
                if Constants.IsPickled is True:
                    if Constants.IsCompressed is True:
                        compressed_data = compress_dumps(data,compression=Constants.CompressionType)
                        cached_Data[key] = compressed_data
                        if Constants.MeasureSize is True:
                            print("Compressed Data Size :: {}".format(get_size(compressed_data)))
                    else:
                        pickled_data = dumps(data)
                        cached_Data[key] = pickled_data
                        
                        if Constants.MeasureSize is True:
                            print("Pickled Data Size :: {}".format(get_size(pickled_data)))
                else:
                    cached_Data[key] = data
                
        elif cache_type_env == Constants.Cache_Type_Distributed: # Distributed Cache
            # rl = redis_lock.Lock(cached_Data,key)
            # try:
            #     rl.acquire(blocking=True)
            with redis_lock.Lock(cached_Data,key):
                if Constants.IsCompressed is True:
                    compressed_data = compress_dumps(data,compression=Constants.CompressionType)
                    cached_Data.set(key,compressed_data) # Cached data is Pickled binary serialization and stored
                    
                    if Constants.MeasureSize is True:
                        print("Compressed Data Size :: {}".format(get_size(compressed_data)))
                else:
                    pickled_data = dumps(data)
                    cached_Data.set(key,pickled_data) # Cached data is Pickled binary serialization and stored
                    
                    if Constants.MeasureSize is True:
                        print("Pickled Data Size :: {}".format(get_size(pickled_data)))
            # finally:
            #     rl.release()          
        else:
            with cache_lock:
                if Constants.IsPickled is True:
                    if Constants.IsCompressed is True:
                        compressed_data = compress_dumps(data,compression=Constants.CompressionType)
                        cached_Data[key] = compressed_data
                        if Constants.MeasureSize is True:
                            print("Compressed Data Size :: {}".format(get_size(compressed_data)))
                    else:
                        pickled_data = dumps(data)
                        cached_Data[key] = pickled_data
                        
                        if Constants.MeasureSize is True:
                            print("Pickled Data Size :: {}".format(get_size(pickled_data)))
                else:
                    cached_Data[key] = data

'''
Sort and Paginate the Cached Data
'''
def CacheSortPaginate(result,
                      cache_key,
                      primaryKey,
                      paginateDictionary,
                      multiProcess:bool=True):
    # For Cached data result needs Sorting and Pagination
                
    if cache_key is not None: # For a valid Cache Key Sort and Paginate
        if primaryKey is not None: # For a Valid PK Sort the Data
            if multiProcess:  #If Process is multiProcess tehn only it required to sort the process,
                                  #In Non-Multi-Process, it will be automatically sorted.
                result = sorted(result,key=SortKeyPick(primaryKey if isinstance(primaryKey,list) else [primaryKey]))
            result = PaginateResult(result,paginateDictionary)
    return result

'''
Update the Cached Data for every DB update
'''
def CacheUpdate(primaryKeyFilter:dict,
                cache_key,
                UpdateDictionary):    
    
    # Fetch Cache Key
    key = GetCacheKey(primaryKeyFilter,cache_key)
    
    # If Key is not None then Update the data
    if key is not None:
        result = GetCacheData(key) # Fetched Cached Data
        
        # For Primary Key update the Selective Data
        if primaryKeyFilter is not None and len(primaryKeyFilter.keys()) > 0 and result is not None:            
            # Traverse through Cached Data
            for d in result:                
                primaryKeyMatch = True
                
                for k,v in primaryKeyFilter.items():
                    if d[k] == v:
                        continue
                    else:
                        primaryKeyMatch = False
                        break
                
                if(primaryKeyMatch):
                    for k,v in UpdateDictionary.items():
                        d[k] = v
                # Update the Cached Data
                SetCacheData(key,result)
        else: # Update all the Cached data (as Primary key isn't specified)
            if result is not None:
                for d in result:
                    for k,v in UpdateDictionary.items():
                            d[k] = v        
                       
                # Update the Cached Data
                SetCacheData(key,result)


'''
Insert the Db inserted data into Cache
'''
def CacheInsert(primaryKeyFilterList,
                cache_key,
                insertDictionaryList):
    
    if primaryKeyFilterList is None or len(primaryKeyFilterList) == 0:
        # Fetch Cache Key
        # For Insertion there's no Primary Key (as its Inserted)
        key = GetCacheKey(None,cache_key)
        
        # If Key is not None then Update the data
        if key is not None:
            result = GetCacheData(key) # Fetched Cached Data
            
            # Insert all data into Cache
            for insertionRow in insertDictionaryList:
                result.append(insertionRow) # Insertion Row Shall Contain the PK 
                            
            # Update the Cached Data
            SetCacheData(key,result)
    else:
        # For Primary Key update the Selective Data
        for primaryKeyFilter in primaryKeyFilterList:
            # Fetch Cache Key
            # For Insertion there's no Primary Key (as its Inserted)
            key = GetCacheKey(primaryKeyFilter,cache_key)
            
            # If Key is not None then Update the data
            if key is not None:
                result = GetCacheData(key) # Fetched Cached Data
                
                # Insert all data into Cache
                for insertionRow in insertDictionaryList:
                    result.append({**primaryKeyFilter,**insertionRow}) # Pk is explicitly merged with the Insertion Rows
                                
                # Update the Cached Data
                SetCacheData(key,result)

'''
Delete the Db Delete Data into Cache.
Takes the Primary key filter list and deletes mapping data.
'''
def CacheDelete(primaryKeyFilterList,
                cache_key):
    
    # For Primary Key update the Selective Data
    for primaryKeyFilter in primaryKeyFilterList:
         
        # Fetch Cache Key
        key = GetCacheKey(primaryKeyFilter,cache_key)
        
        # If Key is not None then Fetch result
        if key is not None:
            result = GetCacheData(key) # Fetched Cached Data
            
            if result is not None:                
                # Traverse through Cached Data
                for d in result:                
                    primaryKeyMatch = True
                    
                    # Check if a Primary Key mapping exist
                    for k,v in primaryKeyFilter.items():
                        if d[k] == v:
                            continue
                        else:
                            primaryKeyMatch = False
                            break

                    # For Mapped Data remove from the collection
                    if(primaryKeyMatch):
                        result.remove(d)            
                            
                # Update the Cached Data
                SetCacheData(key,result)

