from collections import OrderedDict
import logging
import numpy as np
from scipy.optimize import curve_fit

from leframework.codestyle import overrides
from leframework.model.state import State

class SegmentationGenerator(State):

    def __init__(self):
        State.__init__(self, "SegmentationGenerator")
        self.logger = logging.getLogger(name='segmentationgenerator')

    @overrides(State)
    def execute(self):
        mediator = self.mediator
        schema = mediator.schema

        orderedScore = self.mediator.data[[schema["reserved"]["score"], schema["target"]]]
        orderedScore.sort([schema["reserved"]["score"], schema["target"]], axis=0, ascending=False, inplace=True)
        numLeads = orderedScore.shape[0]

        # Break Into 1% Granularity
        numSegments = 100
        numBlocks = numSegments
        blockSize = numLeads / numBlocks
        remainder = numLeads - (numBlocks * blockSize)
        segments = [0] * numBlocks

        offset = [0]
        def addSegment(i):
            lower = offset[0]; upper = min(lower + blockSize, numLeads)
            segmentScores = orderedScore[lower:upper]
            segment = OrderedDict()
            segment["Score"] = (100 - i)
            segment["Count"] = segmentScores.shape[0]
            segment["Converted"] = int(segmentScores[schema["target"]].sum())
            segments[i] = segment
            offset[0] = upper

        if remainder > 0:
            blockSize += 1
            for i in xrange(0, remainder):
                addSegment(i)
            blockSize -= 1

        for i in xrange(remainder, numBlocks):
            addSegment(i)

        # Apply Curve Fit
        self.applyCurveFit(segments)

        # Construct Result
        self.result = []
        allSegments = OrderedDict()
        allSegments["LeadSource"] = "All"
        allSegments["Segments"] = segments
        self.result.append(allSegments)

        # Add Result to Mediator
        self.mediator.segmentations = self.result

    def applyCurveFit(self, segments):
        '''
        Good Enough (For Now)
        '''
        def model(x, a, b, c):
            return a * np.exp(-b * x) + c

        xdata = range(len(segments))
        ydata = [float(s["Converted"]) / float(s["Count"]) if s["Count"] > 0 else 0 for s in segments]

        try:
            popt, _ = curve_fit(model, xdata, ydata)
            yFitdata = [model(i, popt[0], popt[1], popt[2]) for i in xdata]
            for i in xdata: segments[i]["FitConversion"] = yFitdata[i]
        except:
            self.logger.warn("Could Not Apply Curve Fit", exc_info=True)
            for i in xdata: segments[i]["FitConversion"] = -1
