import copy
import os
import sys

from bucketconsolidator import BucketConsolidator
from geometricbucketer import GeometricBucketer
import pkgutil as pkg


class BucketerDispatcher(object):

    def __init__(self):
        self.suffix = 'bucketer'
        self.__importAllBucketers()
        # Set up default bucketing strategy
        self.defaultBucketer = GeometricBucketer()
        self.defaultMaxBuckets = 7
        self.defaultParams = {'minValue': 1, 'multiplierList': [2, 2.5, 2]}
        self.defaultLiftThreshold = 0.2
        self.defaultRatioThreshold = 0.2

    def __importAllBucketers(self):
        curDir = os.path.dirname(os.path.abspath(__file__))
        for _, name, _ in pkg.iter_modules([curDir]):
            if name.endswith(self.suffix) and len(name) > len(self.suffix):
                # Get class name from module name, i.e linearbucketer -> LinearBucketer
                className = name.replace(self.suffix, '').title() + self.suffix.title()
                if className not in globals():
                    path = list(sys.path)
                    sys.path.append(curDir)
                    try:
                        # Set the module name in the current global name space:
                        mod = __import__(name, fromlist=[className])
                        globals()[className] = getattr(mod, className)
                    finally:
                        # Restore
                        sys.path[:] = path

    def bucketColumn(self, columnSeries, eventSeries, methodType=None, methodParams=None):
        '''
        Input i.e. methodType = 'linear' -> method = 'LinearBucketer'
        If the bucketing method is not found, default bucketing algorithm will be invoked
        '''
        bucketer = self.defaultBucketer
        maxBuckets = self.defaultMaxBuckets
        liftThreshold = self.defaultLiftThreshold
        ratioThreshold = self.defaultRatioThreshold
        params = self.defaultParams

        if methodType is not None:
            method = methodType.title() + self.suffix.title()
            if method in globals():
                bucketerClass = globals()[method]
                bucketer = bucketerClass()
                # Set type specific parameters
                copyParams = copy.deepcopy(methodParams)
                maxBuckets = copyParams.pop("maxBuckets", maxBuckets)
                liftThreshold = copyParams.pop("liftThreshold", liftThreshold)
                ratioThreshold = copyParams.pop("ratioThreshold", ratioThreshold)
                params = copyParams

        bucketList = bucketer.bucketColumn(columnSeries, eventSeries, params)

        if bucketer.doConsolidation():
            bc = BucketConsolidator(maxBuckets, liftThreshold, ratioThreshold)
            return bc.consolidateBins(columnSeries, eventSeries, bucketList)
        
        return bucketList
