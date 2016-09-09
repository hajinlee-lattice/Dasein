import logging

from bucketer import Bucketer
from leframework.codestyle import overrides
import numpy as np


class GeometricBucketer(Bucketer):

    def __init__(self):
        self.logger = logging.getLogger(name = 'geometricbucketer')

    @overrides(Bucketer)
    def bucketColumn(self, columnSeries, eventSeries, params):
        return [x for x in self.generateGeometricBins(columnSeries, **params)]

    @staticmethod
    def __generationHelper(columnSeries, minValue, multiplierList, minSamples, minFreq, maxPercentile):
        """
        Generator function that takes a pandas Series and attempts to carve it into semi-geometric regions:
            [0, minValue], [minValue, minValue*muliplierList[i % len(multiplierList)], etc.
        
        Stops generating if a bin contains less than minSamples rows or (n / 10 * maxBuckets) rows
        
        Yields:
            List of thresholds for the bins
        """
        populatedRows = columnSeries[columnSeries.notnull()]
        minRows = max([1, minSamples, minFreq * columnSeries.count()])
        remainingCount = populatedRows.count()
    
        maxValue = columnSeries.quantile(maxPercentile)
    
        if minValue <= 0:
            raise ValueError("minValue cannot be less than or equal to zero.")
    
        if any([(x <= 1) for x in multiplierList]):
            raise ValueError("multiplierList cannot contain values <= 1.")
    
        yield 0
    
        currentValue = minValue
        lastBinValue = 0
        i = 0
        while (currentValue < maxValue):
            possibleBinCount = populatedRows[(populatedRows >= lastBinValue) & (populatedRows < currentValue)].count()
    
            # make sure that this bin won't be below the minimum sample count 
            # and that it is a reasonable value given the number of buckets
            if possibleBinCount >= minRows and (remainingCount - possibleBinCount) >= minRows:
                remainingCount -= possibleBinCount
                lastBinValue = currentValue
                yield currentValue
    
            currentValue *= multiplierList[i % len(multiplierList)]
            i += 1
    
        yield np.inf


    def generateGeometricBins(self, columnSeries, minValue, multiplierList, minSamples=100, minFreq=0, maxPercentile=1, includeNegatives=False):
        positiveBins = [x for x in GeometricBucketer.__generationHelper(columnSeries, minValue, multiplierList, minSamples, minFreq, maxPercentile)]
        negativeBins = []
        if includeNegatives:
            negativeBins = [-x for x in GeometricBucketer.__generationHelper(-columnSeries, minValue, multiplierList, minSamples, minFreq, maxPercentile)]    
    
        # remove duplicate values    
        return_value = list(set(positiveBins + negativeBins))
        return_value.sort()
        return return_value