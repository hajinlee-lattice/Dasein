import os
import sys
import pkgutil as pkg
import numpy as np
import scipy.stats
from geometricbucketer import GeometricBucketer

class BucketerDispatcher(object):

    def __init__(self):
        self.suffix = 'bucketer'
        self.__importAllBucketers()
        # Set up default bucketing strategy
        self.defaultBucketer = GeometricBucketer()
        self.defaultMaxBuckets = 7
        self.defaultParams = {'minValue': 1, 'multiplierList': [2, 2.5, 2]}

    def __importAllBucketers(self):
        curDir = os.path.dirname(os.path.abspath(__file__))
        for _, name, _ in pkg.iter_modules([curDir]):
            if name.endswith(self.suffix) and len(name) > len(self.suffix):
                # Get class name from module name, i.e linearbucketer -> LinearBucketer
                className = name.replace(self.suffix,'').title()+self.suffix.title()
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

    def bucketColumn(self, columnSeries, eventSeries, methodType = None, methodParams = None):
        '''
        Input i.e. methodType = 'linear' -> method = 'LinearBucketer'
        If the bucketing method is not found, default bucketing algorithm will be invoked
        '''
        bucketer =  self.defaultBucketer
        maxBuckets = self.defaultMaxBuckets
        params = self.defaultParams

        if methodType is not None:
            method = methodType.title() + self.suffix.title()
            if method in globals():
                bucketerClass = globals()[method]
                bucketer = bucketerClass()
                # Set type specific parameters
                maxBuckets = methodParams.pop("maxBuckets", maxBuckets)
                params = methodParams

        bucketList = bucketer.bucketColumn(columnSeries, params)
        bucketList = self.consolidateBins(columnSeries, eventSeries, bucketList, maxBuckets)
        # Reset infinity to None
        return map(lambda x: None if np.isinf(x) else x, bucketList)

    def consolidateBins(self, columnSeries, eventSeries, bucketList, maxBuckets):
        """
        Method to consolidate a list of continuous attribute buckets given an attribute column and event column.  Ensures that the bucket count is 
        less than maxBuckets by removing adjacent ranges that have the maximum probability of resulting from a binomial distribution of the combined 
        ranges
        """
        if maxBuckets <= 0:
            raise ValueError("maxBuckets cannot be less than or equal to zero.")

        zippedList = zip(bucketList, bucketList[1::])    
        getSelector = lambda x, nextX: (columnSeries > x) & (columnSeries <= nextX)
        eventSums = [(eventSeries[getSelector(x, nextX)].sum(), np.count_nonzero(getSelector(x, nextX))) for x, nextX in zippedList]

        while (len(zippedList) > maxBuckets):
            maxDelta = 0
            removeIndex = 0
            for i in range(len(zippedList)-1):
                newConversionRate = float(eventSums[i][0] + eventSums[i + 1][0] + 1) / float(eventSums[i][1] + eventSums[i + 1][1] + 1)
                delta = scipy.stats.binom.pmf(eventSums[i][0], eventSums[i][1], newConversionRate) * scipy.stats.binom.pmf(eventSums[i + 1][0], eventSums[i + 1][1], newConversionRate)
                if np.isnan(delta):
                    raise RuntimeError("Invalid event series distribution.")
                if delta > maxDelta:
                    maxDelta = delta
                    removeIndex = i

            # combine the bins around the removed threshold 
            zippedList[removeIndex] = (zippedList[removeIndex][0], zippedList[removeIndex + 1][1])
            eventSums[removeIndex] = (eventSums[removeIndex][0] + eventSums[removeIndex + 1][0], eventSums[removeIndex][1] + eventSums[removeIndex + 1][1])
            zippedList.remove(zippedList[removeIndex + 1])
            eventSums.remove(eventSums[removeIndex + 1])

        return [y[0] for y in zippedList] + [zippedList[-1][1]]    
