from abc import ABCMeta, abstractmethod

class State(object):
    __metaclass__ = ABCMeta
    
    @abstractmethod
    def execute(self): pass
    
    @abstractmethod
    def getKey(self):
        return None