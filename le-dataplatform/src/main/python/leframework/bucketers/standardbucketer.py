import logging
from bucketer import Bucketer
from leframework.codestyle import overrides
import numpy as np
import pandas.core.algorithms as algos

class StandardBucketer(Bucketer):


    def __init__(self):
        self.logger = logging.getLogger(name='standardbucketer')
    
    @overrides(Bucketer)
    def bucketColumn(self, columnSeries, eventSeris, params):
        
        return self.getStandardBins(columnSeries, **params)


    def getStandardBins(self, columnSeries, numBins=10, minSamples=100, minFreq=0, maxPercentile=1):
        """
        Function to carve a pandas series up into a standard set of bins.  Will try to fit the observed distribution as evenly 
        as possible into numBins bins
        
        Returns:
            List of thresholds for the bins
        """
        populatedRows = columnSeries[columnSeries.notnull()]
        maxValue = populatedRows.quantile(maxPercentile)
        populatedRows = populatedRows[populatedRows <= maxValue]

        minRows = max([1, minSamples, minFreq * columnSeries.count()])    

        ranges = np.linspace(0, 1, numBins + 1)
        betterBins = algos.quantile(populatedRows, ranges)

        # combine adjacent buckets of equal value and try to break up the remaining range evenly
        for i in range(numBins):
            if betterBins[i] == betterBins[i + 1]:
                remainingRanges = np.linspace(0, 1, numBins - i)
                betterBins[i + 1:] = algos.quantile(populatedRows[populatedRows > betterBins[i]], remainingRanges)

        betterBins[0] = -np.inf
        betterBins[-1] = np.inf
        betterBins = betterBins[~np.isnan(betterBins)]

        binThresholds = zip(betterBins, betterBins[1::])    
        binCounts = [populatedRows[(populatedRows >= x[0]) & (populatedRows < x[1])].count() for x in binThresholds]
        binData = zip(binThresholds, binCounts)

        while (any(x[1] < minRows for x in binData) and (len(binData) > 1)):
            smallestBin = min(binData, key=lambda x: x[1])
            smallestIndex = binData.index(smallestBin)

            if smallestIndex != 0:        
                # if we're not the first item in the list then combine with prior bin 
                binData[smallestIndex - 1] = ((binData[smallestIndex - 1][0][0], binData[smallestIndex][0][1]), binData[smallestIndex - 1][1] + binData[smallestIndex][1])
            else:
                # if we're the first item in the list then combine with next bin
                binData[smallestIndex + 1] = ((binData[smallestIndex][0][0], binData[smallestIndex + 1][0][1]), binData[smallestIndex + 1][1] + binData[smallestIndex][1])

            binData.remove(smallestBin)    

        # unpack the thresholds from the bin data
        betterBins = [y[0] for y in [x[0] for x in binData]] + [np.inf]
        return betterBins
