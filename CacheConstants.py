
## Caching (all lower case as these are comparison values at runtime)
Cache_Type_Env_Variable = "cache_type"
Cache_Type_Local = "local"
Cache_Type_Distributed = "distributed"
Cache_Distributed_Server = "cache_server"
Cache_Distributed_Port = "cache_port"
Cache_Distributed_Server_Local = "localhost"
Cache_Distributed_Port_Local = 6379

## Caching Filters
Cache_Filters_Colname = "Colname"
Cache_Filters_Value = "Value"
Cache_Filters_Operator = "Operator"
Cache_Filters_Type = "Type"
Cache_Filters_JsonColname = "Jsoncolumn"
Cache_Filters_Date = "Date"
Cache_Filters_Date_Column = "DateColumns"
Cache_Sorting_PrimaryKey_Column = "PrimaryKeyColumns" # Not Used
Cache_Key_Attributes = "-attributes" # Not Used

## Cache Processing
Cache_Key_Filters_NoMultiProcessing = ["store-master","store-master-attributes"] # Not Used

## Json Filters
Json_Master = "JsonMaster" # Not Used
Json_Attributes = "JsonAttribute" # Not Used
Json_Name = "Name" 
Json_Value = "Value"
Json_Prefix = "Prefix"

# Operators and Pagintion

EqualOp = "=="
PaginateRowIndex = "PaginateRowIndex"
PaginateRowCount = "PaginateRowCount"
PaginateColumn = "PaginateColumn"
PaginateCount = "PaginateCount"
PaginateIndex = "PaginateIndex"
PaginateValue = "PaginateValue"

# Caching Options

IsPickled = True
IsCompressed = False
Gzip = "gzip"
Bz2 = "bz2"
Lzma = "lzma"
Zipfile = "zipfile"
Lz4 = "lz4"
CompressionType = Bz2
MeasureSize = False


