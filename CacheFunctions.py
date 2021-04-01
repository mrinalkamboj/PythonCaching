from CacheInit import cached_Data,cache_type_env

import CacheConstants as Constants

'''
Delete Cache Data by Key in both local and distributed.
Removing a specific Key from the Cache
'''
def DeleteCacheDataByKey(key:str):
    
    if cache_type_env == Constants.Cache_Type_Local: # Local Cache
        cached_Data.pop(key,None) # Remove a Key from the Local Dictionary or returns None
    elif cache_type_env == Constants.Cache_Type_Distributed: # Distributed Cache
        cached_Data.delete(key) # Remove a Key from the Distributed Cache
    else:
        cached_Data.pop(key,None)

'''
Clear All Cache Data / All Keys
'''
def ClearAllCacheData():
    
    if cache_type_env == Constants.Cache_Type_Local: # Local Cache
        cached_Data = {} # Empty Dictionary
    elif cache_type_env == Constants.Cache_Type_Distributed: # Distributed Cache
        for key in cached_Data.keys():
            cached_Data.delete(key)  # Fetch and Delete all Cache Keys in the distributed Cache
    else:
        cached_Data = {}

'''
Cache Test Method, Fetch all Keys in the Cache.
'''
def FetchCacheKeys():
    # Both Local and Distributed Cache (Redis) have same API to return data
    return cached_Data.keys()