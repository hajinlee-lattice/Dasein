from abc import ABCMeta, abstractmethod

class Bucketer(object):
    '''
    Base class for bucketing columns for visualization.
    '''
    __metaclass__ = ABCMeta

    def __init__(self): pass

    @abstractmethod
    def bucketColumn(self, columnSeries, eventSeries, params): pass
    
    def doConsolidation(self):
        return True

    def getSubClasses():
        classList = []
        for classType in Bucketer.__subclasses__():
            classStr = classType.__name__
            classList.append(classStr)
        return classList

    getSubClasses = staticmethod(getSubClasses)