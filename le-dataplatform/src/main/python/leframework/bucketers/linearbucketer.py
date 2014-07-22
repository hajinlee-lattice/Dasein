import logging
import math
import numpy as np
from bucketer import Bucketer
from leframework.codestyle import overrides


class LinearBucketer(Bucketer):


    def __init__(self):
        self.logger = logging.getLogger(name = 'linearbucketer')
    
    @overrides(Bucketer)
    def bucketColumn(self, *args):    
        
        return [x for x in self.generateLinearBins(*args)]
    
                   
    def generateLinearBins(self, columnSeries, minValue, stepSize, minSamples = 0, minFreq = 0, maxPercentile = 1, maxIteration = 1000):
        """
        Generator function that takes a pandas Series and attempts to carve it into linear regions defined by the starting point and 
        and a step size        
        
        Yields:
            List of thresholds for the bins
        """
        if minValue < 0:
            raise ValueError("minValue cannot be less than zero")
                
        populatedRows = columnSeries[columnSeries.notnull()]
        minRows = max([1, minSamples, minFreq * columnSeries.count()])      
        remainingCount = populatedRows.count()
        
        yield 0
    
        if (columnSeries.quantile(maxPercentile) - minValue) / float(stepSize) > maxIteration:
            stepSize = max(1, 10 ** (round(math.log10(max(columnSeries.max(), 1)) - 2)))
        
        lastBinValue = 0
        for i in np.arange(minValue, columnSeries.quantile(maxPercentile), stepSize):
            possibleBinCount = populatedRows[(populatedRows > lastBinValue) & (populatedRows <= i)].count()
            
            # make sure that this bin won't be below the minimum sample count 
            # and that it is a reasonable value given the number of buckets
            if possibleBinCount >= minRows and (remainingCount - possibleBinCount) >= minRows:
                # handle the case where the user specified a minValue of 0
                if i != 0:
                    yield i
                lastBinValue = i
                remainingCount -= possibleBinCount
                
        yield np.inf
