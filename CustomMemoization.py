import enum
from redis import Redis
import pickle

# Creating Cache Decorator (currently unused and unfinished functionality)
class CacheType(enum.Enum):
   Local = 1
   Distributed = 2

myCache = None

cacheType = CacheType.Distributed

if cacheType == CacheType.Local:
    myCache = dict()
else:    
    myCache = Redis(host='localhost', port=6379, db=0)

def memoize(func):
    
    def memoized_func(**kwargs):
        
        key = kwargs["val"]
        print("KeyValue {key}".format(key=key))
        
        if cacheType == CacheType.Local:
            
            if key in myCache:
                return myCache[key]
        else:
            cachedResult = myCache.get(key)
            
            if cachedResult is not None:
                return pickle.loads(cachedResult)
        
        # Actual Function Execution
        result = func(**kwargs)
        
        if cacheType == CacheType.Local:
            myCache[key] = result
        else:
            myCache.set(key,pickle.dumps(result))
        
        return result

    return memoized_func
