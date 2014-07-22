from bucketer import Bucketer
# Import all subclasses used by globals()
from standardbucketer import StandardBucketer
from linearbucketer import LinearBucketer    # @UnusedImport
from geometricbucketer import GeometricBucketer # @UnusedImport
from fixedbucketer import FixedBucketer # @UnusedImport


class BucketerDispatcher(object):

    def __init__(self):
        self.suffix = 'Bucketer'
        self.defaultBucketer = StandardBucketer()
        self.bucketers = Bucketer.getSubClasses()

    '''
    Input i.e. methodType = 'linear' -> method = 'LinearBucketer'
    If the bucketing method is not found, default bucketing algorithm will be invoked
    '''
    def bucketColumn(self, columnSeries, methodType = None, methodParams = None):
        bucketer =  self.defaultBucketer
        params = [columnSeries]

        if methodType is not None:
            method = methodType.title() + self.suffix
            if method in self.bucketers:
                bucketerClass = globals()[method]
                bucketer = bucketerClass()
                params.extend(methodParams)

        return bucketer.bucketColumn(*params)
