from collections import OrderedDict
import logging

from leframework.codestyle import overrides
from leframework.model.state import State
from leframework.util.scoringutil import ScoringUtil

class SegmentationGenerator(State):

    def __init__(self):
        State.__init__(self, "SegmentationGenerator")
        self.logger = logging.getLogger(name='segmentationgenerator')

    @overrides(State)
    def execute(self):
        mediator = self.mediator
        schema = mediator.schema

        orderedScore = self.mediator.data[[schema["reserved"]["score"], schema["target"]]]
        orderedScore = ScoringUtil.sortWithRandom(orderedScore, 0)
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

        # Construct Result
        self.result = []
        allSegments = OrderedDict()
        allSegments["LeadSource"] = "All"
        allSegments["Segments"] = segments
        self.result.append(allSegments)

        # Add Result to Mediator
        self.mediator.segmentations = self.result
