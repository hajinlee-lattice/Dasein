from abc import ABCMeta, abstractmethod

class Executor(object):
    '''
    Base class for executing a data processing flow
    '''
    __metaclass__ = ABCMeta

    def __init__(self): pass

    @abstractmethod
    def transformData(self, params): pass
    
    @abstractmethod
    def postProcessClassifier(self, clf, params): pass

    @abstractmethod
    def getModelDirPath(self, schema): pass