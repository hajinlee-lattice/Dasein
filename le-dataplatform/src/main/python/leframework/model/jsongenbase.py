from abc import ABCMeta, abstractmethod

class JsonGenBase(object):
    __metaclass__ = ABCMeta
    
    @abstractmethod
    def getKey(self): pass
    
    @abstractmethod
    def getJsonProperty(self): pass

    

