#THIS IS A FILE TO BE PUT IN THE SAME DIRECTORY AS THE OTHER FUNCTIONS
#These helper functions are called by some of the other functions

## helper function
def isNumber(x):
    try:
        y=float(x)
        return True
    except:
        return False
    
unusualCharacterSet=set(['!', '@', '#', '"', '%', '$', '`', '}', '+', '*', '\\', '^', '~', '_', '{', '=', '<', '?', '>'])
def DS_HasUnusualChar(x):
   if len(set(x) & unusualCharacterSet)!=0: return True
   if isNumber(x): return True
   badSet= set(['none','no','not','delete','asd','sdf','unknown','undisclosed','null','don','abc','xyz','nonname','nocompany'])
   for y in badSet:
       if y in x.lower() and len(x)-len(y)<3: return True
   return False

def valueReturn(x,mappingList):
    if x is None: return 'Null'
    if len(x)==0: return 'Null'    
    if DS_HasUnusualChar(x): return ''
    y=x.lower()
    for title,keys  in mappingList:
        if any(z in y for z in keys):   return title    
    return 'Other'
