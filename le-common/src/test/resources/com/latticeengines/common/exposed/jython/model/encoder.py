from abc import ABCMeta, abstractmethod
import math

class Encoder(object):
    __metaclass__ = ABCMeta
    
    def __init__(self, name):
        self.name = name

    @abstractmethod
    def transform(self, dataFrame): pass
    
class HashEncoder(Encoder):

    def __init__(self):
        Encoder.__init__(self, "HashEncoder")
    
    def transform(self, dataFrame):
        return dataFrame.apply(lambda x: encode(x))
    
def encode(x):
    '''
        This uses the sdbm algorithm targeted for uniqueness, not as a secure hash like SHA or MD5. 
        Collisions are possible especially since we are only getting the first 4 bytes
    '''
    if x is None:
        x = 'NULL'
        
    if isinstance(x, (int, long, float)):
        if math.isnan(x):
            x = 'NULL'
        else:
            return x
    try:
        return int(0xffffffff & reduce(lambda h,c: ord(c) + (h << 6) + (h << 16) - h, x, 0))
    except Exception:
        print("Error with type = %s and value = %s" % (type(x), x))
        raise
    
def transform(args, record):
    column = args["column"]
    value = record[column]

    return int(encode(value))
    
