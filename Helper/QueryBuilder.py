import CacheConstants as Constants

'''
Fetch Operator for the where query
'''
def FetchOperator(colname:str,
                  operatorsDict,
                  jsonPrefix:str):
    
    # Default Equal Operator
    operator = Constants.EqualOp 
    # Fetch the Operator name from the Dictionary based on column name
    if operatorsDict is not None and isinstance(colname,list) is False: # If Operators Dictionary is not null
        if jsonPrefix is None and colname in operatorsDict: # Non Json Columns
            operator = operatorsDict[colname]
        elif jsonPrefix is not None: #Json Columns
            jsonColNameList = [jsonPrefix,'->>',colname]
            jsonColName = ''.join(jsonColNameList)
            
            if jsonColName in operatorsDict:
                operator = operatorsDict[jsonColName]
    
    return operator