import maya 
import time # Time Profiling
import multiprocessing # Multi Processing
import os
import math
from re import search
from Helper.QueryBuilder import FetchOperator
import CacheConstants as Constants

'''
Process Number Filters
'''
def ExecuteNumberFilters(colValue,
                        operator:str,
                        filterValue):

    returnVal = None
    
    localValue = filterValue
    
    if operator == "=":
        returnVal = colValue == localValue  
    elif operator == ">=":
        returnVal = colValue >= localValue
    elif operator == ">":
        returnVal = colValue > localValue 
    elif operator == "<=":
        returnVal = colValue <= localValue
    elif operator == "<":
        returnVal = colValue < localValue
    else:
        returnVal = colValue == localValue
    
    return returnVal

'''
Process String Filters
'''
def ExecuteStringFilters(colValue,
                        operator:str,
                        filterValue:str,
                        valueType=None):

    returnVal = None
    
    localValue = str(filterValue)
    
    if valueType is None:
        
        if operator == "=":
            returnVal = colValue == localValue  
        elif operator.lower() == "contains":
            returnVal = True if search(localValue,str(colValue)) else False
                
    elif valueType.lower() == "date":
        
        if operator == "=":         
            returnVal = maya.parse(colValue).date == maya.parse(localValue).date
        elif operator == ">":
            returnVal = maya.parse(colValue).date > maya.parse(localValue).date
        elif operator == ">=":
            returnVal = maya.parse(colValue).date >= maya.parse(localValue).date
        elif operator == "<":
            returnVal = maya.parse(colValue).date < maya.parse(localValue).date
        elif operator == "<=":
            returnVal = maya.parse(colValue).date <= maya.parse(localValue).date
        elif operator == "between":
            returnVal = maya.parse(localValue[0]).date <= maya.parse(colValue).date <= maya.parse(localValue[1]).date    
    
    return returnVal

'''
Process List Filters
'''
def ExecuteListFilters(colValue,
                     operator:str,
                     filterValue):

    returnVal = None
    
    localValue = filterValue
    
    if operator.lower() == "contains":
        if isinstance(localValue,str):
            returnVal = localValue in list(colValue)
        else: # Assumption that Local Value is a list
            # returnVal =  set(localValue).issubset(set(data[colName]))
            returnVal =  all(item in list(colValue) for item in localValue)
    
    return returnVal

'''
Process Json Filters
'''
def ExecuteJsonFilters(data:dict,
                       colName:str,                       
                       operator:str, 
                       value,
                       valueType,
                       jsonColName):

    jsonColValue = data[colName.lower()][jsonColName.lower()]
    
    if isinstance(value,int) or isinstance(value,float):
        return ExecuteNumberFilters(jsonColValue,operator,value)
    elif isinstance(value,str) and isinstance(jsonColValue,str):
        return ExecuteStringFilters(jsonColValue,operator,value,valueType)
    elif isinstance(value,str) and isinstance(jsonColValue,list):
        return ExecuteListFilters(jsonColValue,operator,value)
    elif isinstance(value,list) and isinstance(jsonColValue,list):
        return ExecuteListFilters(jsonColValue,operator,value)
    elif isinstance(value,list) and isinstance(jsonColValue,str):
        return ExecuteStringFilters(jsonColValue,operator,value,valueType)

'''
Process All Filters
'''
def ExecuteFilters(data:dict,colName:str,operator:str,value,valueType=None,jsonColName=None):
    
    if jsonColName is not None:
        return ExecuteJsonFilters(data,colName,operator,value,valueType,jsonColName)
    elif isinstance(value,int) or isinstance(value,float):
        return ExecuteNumberFilters(data[colName.lower()],operator,value)
    elif isinstance(value,str) and isinstance(data[colName],str):
        return ExecuteStringFilters(data[colName.lower()],operator,value,valueType)
    elif isinstance(value,str) and isinstance(data[colName.lower()],list):
        return ExecuteListFilters(data[colName.lower()],operator,value)
    elif isinstance(value,list) and isinstance(data[colName],list):
        return ExecuteListFilters(data[colName.lower()],operator,value)
    elif isinstance(value,list) and isinstance(data[colName],str):
        return ExecuteStringFilters(data[colName.lower()],operator,value,valueType)

'''
Prepare Filters
'''
def PrepareFilters(masterFilters:dict,
                   attributeFilters:dict,
                   operators:dict,
                   JsonMasterFilters:dict,
                   JsonAttrFilters:dict):
    
    datafilters = []
    
    if masterFilters is not None and len(masterFilters):
        for key,val in masterFilters.items():
            filter = {}
            filter[Constants.Cache_Filters_Colname] = key
            filter[Constants.Cache_Filters_Operator] = FetchOperator(key,operators,None)
            filter[Constants.Cache_Filters_Value] = val   
            filter[Constants.Cache_Filters_Type] = Constants.Cache_Filters_Date.lower() if Constants.Cache_Filters_Date_Column in operators and key in operators[Constants.Cache_Filters_Date_Column] else None
            filter[Constants.Cache_Filters_JsonColname] = None
            datafilters.append(filter)
    
    if attributeFilters is not None and len(attributeFilters):
        for key,val in attributeFilters.items():
            filter = {}
            filter[Constants.Cache_Filters_Colname] = key
            filter[Constants.Cache_Filters_Operator] = FetchOperator(key,operators,None)
            filter[Constants.Cache_Filters_Value] = val   
            filter[Constants.Cache_Filters_Type] = Constants.Cache_Filters_Date.lower() if Constants.Cache_Filters_Date_Column in operators and key in operators[Constants.Cache_Filters_Date_Column] else None
            filter[Constants.Cache_Filters_JsonColname] = None
            datafilters.append(filter)
    
    if JsonMasterFilters is not None and len(JsonMasterFilters):
        for jsonData in JsonMasterFilters:
            filter = {}
            name = jsonData[Constants.Json_Name]
            value = jsonData[Constants.Json_Value]
            prefix = jsonData[Constants.Json_Prefix]
            # # jsoncolumnName = "{JsonColPrefix}{JsonColJoin}{JsonColName}".format(JsonColPrefix=prefix,
            # #                                                                     JsonColJoin="->>",
            # #                                                                     JsonColName=name)
            filter[Constants.Cache_Filters_Colname] = name
            filter[Constants.Cache_Filters_Operator] = FetchOperator(name,operators,prefix)
            filter[Constants.Cache_Filters_Value] = value   
            filter[Constants.Cache_Filters_Type] = Constants.Cache_Filters_Date.lower() if Constants.Cache_Filters_Date_Column in operators and key in operators[Constants.Cache_Filters_Date_Column] else None
            filter[Constants.Cache_Filters_JsonColname] = prefix
            datafilters.append(filter)
    
    if JsonAttrFilters is not None and len(JsonAttrFilters):
        for jsonData in JsonAttrFilters:
            filter = {}
            name = jsonData[Constants.Json_Name]
            value = jsonData[Constants.Json_Value]
            prefix = jsonData[Constants.Json_Prefix]
            # # jsoncolumnName = "{JsonColPrefix}{JsonColJoin}{JsonColName}".format(JsonColPrefix=prefix,
            # #                                                                     JsonColJoin="->>",
            # #                                                                     JsonColName=name)
            filter[Constants.Cache_Filters_Colname] = name
            filter[Constants.Cache_Filters_Operator] = FetchOperator(name,operators,prefix)
            filter[Constants.Cache_Filters_Value] = value   
            filter[Constants.Cache_Filters_Type] = Constants.Cache_Filters_Date.lower() if Constants.Cache_Filters_Date_Column in operators and key in operators[Constants.Cache_Filters_Date_Column] else None
            filter[Constants.Cache_Filters_JsonColname] = prefix
            datafilters.append(filter)
        
    
    return datafilters


datafilters = [] # Filtering Data

'''
Data Filtering, apply all filters as And to every data point
'''
def DataFilterProcessing(dataPoint):    
    if all([ExecuteFilters(dataPoint,
                           f[Constants.Cache_Filters_Colname.lower()],
                           f[Constants.Cache_Filters_Operator.lower()],
                           f[Constants.Cache_Filters_Value.lower()],
                           f[Constants.Cache_Filters_Type.lower()],
                           f[Constants.Cache_Filters_JsonColname.lower()]) for f in datafilters]):
        return dataPoint
    else:
        return


'''
Fetch the CPU engagement, keeping the base size per CPU as 50 K
'''

max_data_per_cpu = 50000

'''
'''
def cpu_engagement(datalength):
    
    cpu_count = math.floor(0.8*os.cpu_count()) # Use 80% of the CPU Capacity
    
    expected_data_size = cpu_count*max_data_per_cpu # Expected Data Size

    if datalength <= max_data_per_cpu: # For low data length turn to single processor
        return 1        
    elif expected_data_size < datalength: # For larger data value return all required cpu, as that's max processing
        return cpu_count
    else:
        return datalength // max_data_per_cpu # For smaller data value, return the modulus


'''
Process Data - Multi-Process / Single Process
'''
def ProcessData(dataTotal,filters,multiProcess:bool=False):
    
    startFilter = time.time() # time profiling
    global datafilters # data filters for filtering data
    datafilters = filters
    filtered_result = None
    
    cpu_count = cpu_engagement(len(dataTotal))

    if cpu_count == 1:
        multiProcess = False
    
    if multiProcess:
        print("Multi-Process")
        pool = multiprocessing.Pool(processes=cpu_count) # Pool for Multi processing
        filtered_result = pool.map(DataFilterProcessing, dataTotal) # Run multi processing per data point
        pool.close()
        pool.join()
        filtered_result = [x for x in filtered_result if x is not None] # Remove None from Filtered data
    else:
        print("Non-Multi-Process")
        filtered_result = list(d for d in dataTotal if all([ExecuteFilters(d,
                                                                           f[Constants.Cache_Filters_Colname.lower()],
                                                                           f[Constants.Cache_Filters_Operator.lower()],
                                                                           f[Constants.Cache_Filters_Value.lower()],
                                                                           f[Constants.Cache_Filters_Type.lower()],
                                                                           f[Constants.Cache_Filters_JsonColname.lower()]) for f in datafilters]))
    
    print("Filter Time % s seconds" % (time.time() - startFilter))
    return filtered_result
    

'''
'''
def FetchFilters():
    
    datafilters = [
                        {
                            "colname":"name",
                            "operator": "contains",
                            "value":"Z",
                            "type":None,
                            "jsoncolumn":None
                            
                        },
                        # # {
                        # #     "colname":"traits",
                        # #     "operator": "contains",
                        # #     "value": ["fair"],
                        # #     "type":None,
                        # #     "jsoncolumn":None
                        # # },
                        # # {
                        # #     "colname":"id",
                        # #     "operator": ">",
                        # #     "value": 10,
                        # #     "type":None,
                        # #     "jsoncolumn":None
                        # # },
                        # # {
                        # #     "colname":"level",
                        # #     "operator": "==",
                        # #     "value": "Architect",
                        # #     "type":None,
                        # #     "jsoncolumn":None
                        # # }    
                  ]
    
    return datafilters


## Local Filter Test

# # localfilters =  {
# #                     "colname":"name",
# #                     "operator": "contains",
# #                     "value":["Mrinal"],
# #                     "type":None,
# #                     "jsoncolumn":"a"                            
# #                 } 
                
    


# # data = {"name":{"a":["Mrinal"]}}

# # final = ExecuteFilters(data,localfilters["colname"],
# #                        localfilters["operator"],
# #                        localfilters["value"],
# #                        localfilters["type"],
# #                        localfilters["jsoncolumn"])

# # print(final)