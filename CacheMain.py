import time
from DataGenerator import generateData
from CacheFilters import ProcessData, FetchFilters

'''
Main Cache Filters Start Method
'''
def CacheFiltersStart():
    cnt = 0 

    startMain = time.time()

    while cnt < len(range(3)):
        
        # Primary Key
        primaryKeyDictionary = dict()
        primaryKeyDictionary["key1"] = "Val1"
        primaryKeyDictionary["key2"] = "Val2"
       
         # Generate Data 
        dataTotal = generateData(primaryKeyDictionary,"MainCache")

        # Fetch Data Filters
        datafilters = FetchFilters()

        # Total Data
        print("Total Data Count - ",len(dataTotal))

        # Data Filtering using "and (all)" between filters - ExecuteFilters 
        # We can also use "Or (any)" between the filters   
        
        # Filter Data
        filtered_result = ProcessData(dataTotal,datafilters,True)
        
        # # print(filtered_result)

        print("Filter Data Count - ",len(filtered_result))

        # # Code Profiler Time Profiling
        # profiler.print_run_stats("FilterTime")
        
        cnt = cnt + 1

    print("Total Time % s seconds" % (time.time() - startMain)) 


if __name__ == "__main__":
    CacheFiltersStart()