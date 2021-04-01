import os

from threading import Lock, RLock

from redis import Redis,StrictRedis

  
import CacheConstants as Constants

# Fetch Cache Type Environment Variable
# if the Environment Variable not set then make it local Cache not distributed
cache_type_env = os.environ.get(Constants.Cache_Type_Env_Variable).lower() if os.environ.get(Constants.Cache_Type_Env_Variable) is not None else Constants.Cache_Type_Local

# Fetch Distributed Cache Server
cache_server = os.environ.get(Constants.Cache_Distributed_Server).lower() if os.environ.get(Constants.Cache_Distributed_Server) is not None else Constants.Cache_Distributed_Server_Local

# Fetch Distributed Cache Port
cache_port = os.environ.get(Constants.Cache_Distributed_Port) if os.environ.get(Constants.Cache_Distributed_Port) is not None else Constants.Cache_Distributed_Port_Local

# Cached Data Object
cached_Data = None

# Cache Lock Object
cache_lock = Lock()

if cache_type_env == Constants.Cache_Type_Local: # Local Cache
    cached_Data = dict()
elif cache_type_env == Constants.Cache_Type_Distributed:   # Distributed Cache 
    cached_Data = Redis(host=cache_server, port=cache_port, db=0)
else:
    cached_Data = dict() # Local Cache Default