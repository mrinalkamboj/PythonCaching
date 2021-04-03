from py_linq import Enumerable
import CacheConstants as Constants


def SortKeyPick(keynames):
    '''
    Fetch the Keynames for Sorting data
    '''    
    def Getit(adict):
       composite = [adict[k] for k in keynames]
       return composite
    return Getit

# # # Paginate the Cache Result
# # def PaginateResult(enumerable:list, 
# #                    paginateDictionary:dict):
    
# #     """
# #     Take the Input as result set (list / enumerable) and paginate dictionary.
# #     Apply the Predicate and Pagination and return the result as {"count":count,"value":[dict]}
# #     """
# #     rowIndex,rowCount,paginateColumn = None,None,None
    
# #     # Fetch the Pagination elements
# #     if paginateDictionary is not None:
# #         rowIndex = paginateDictionary.get(Constants.PaginateRowIndex)
# #         rowCount = paginateDictionary.get(Constants.PaginateRowCount)
# #         paginateColumn = paginateDictionary.get(Constants.PaginateColumn)
    
# #     # List count
# #     enumerable = Enumerable(enumerable)
# #     count = enumerable.count()

# #     # First Page
# #     if rowIndex is None or rowCount is None:
# #         return list(enumerable)
# #     else:  # 2nd to nth Page 
# #         # If there's no Pagionation Column
# #         if paginateColumn is not None:
# #             # Add rowcount to the supplied index
# #             returnIndex = rowIndex + rowCount
# #             # Fetch Current and Last Value
# #             currentVal, lastVal = None, None
            
# #             # Validate Index Value to avoid runtime exception
# #             if enumerable[returnIndex] is not None:                
# #                 currentVal = enumerable[returnIndex][paginateColumn] 

# #             if enumerable[returnIndex-1] is not None:
# #                 lastVal = enumerable[returnIndex-1][paginateColumn]
            
# #             # Iterate till the point Current and last values are equal
# #             while currentVal is not None and lastVal is not None and lastVal == currentVal:
# #                 returnIndex = returnIndex + 1 # Increment the Index
                
# #                 # Validate Index Value to avoid runtime exception
# #                 if enumerable[returnIndex] is not None:
# #                     currentVal = enumerable[returnIndex][paginateColumn]
# #                     lastVal = enumerable[returnIndex-1][paginateColumn]
# #                 else:
# #                     currentVal = None
                    
# #             # Return Special Pagination Result    
# #             return {
# #                 Constants.PaginateCount: count,
# #                 Constants.PaginateIndex: returnIndex, # Index till where Records match
# #                 Constants.PaginateValue: list(enumerable.skip(rowIndex).take(returnIndex-rowIndex)),
# #             }
# #         else: # Return Standard Pagination Result 
# #             return {
# #                 Constants.PaginateCount: count,
# #                 Constants.PaginateIndex: rowIndex + rowCount,
# #                 Constants.PaginateValue: list(enumerable.skip(rowIndex).take(rowCount)),
# #             }

