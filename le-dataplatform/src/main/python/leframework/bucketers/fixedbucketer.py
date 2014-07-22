import logging
import numpy as np
from bucketer import Bucketer
from leframework.codestyle import overrides


class FixedBucketer(Bucketer):


    def __init__(self):
        self.logger = logging.getLogger(name = 'fixedbucketer')

    @overrides(Bucketer)
    def bucketColumn(self, *args):

        return [x for x in self.generateFixedBins(*args)]

    def generateFixedBins(self, columnSeries, numbins = 10):
        """
        Generator function that takes a pandas Series and attempts to carve it into evenly spaced regions
        
        Yields:
            List of thresholds for the bins
        """
        if numbins < 1:
            raise ValueError("numbins cannot be less than or equal to one")
        
        yield -np.inf
        
        lastThreshold = columnSeries.min()    
        bucketSize = (columnSeries.max() - columnSeries.min()) / float(numbins)
        if bucketSize == 0:
            yield np.inf
            return
        
        for _ in range(numbins - 1):
            lastThreshold += bucketSize
            yield lastThreshold        
        
        yield np.inf
