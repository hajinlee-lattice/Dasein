import itertools
import logging
import numpy as np
from leframework.codestyle import overrides
from leframework.model.state import State
from leframework.util.pdversionutil import pd_before_17


class ROCGenerator(State):

    def __init__(self):
        State.__init__(self, "ROCGenerator")
        self.logger = logging.getLogger(name='rocgenerator')

    @overrides(State)
    def execute(self):
        mediator = self.mediator
        schema = mediator.schema

        score = mediator.data[[schema["reserved"]["score"], schema["target"]]]
        rows = score.shape[0]

        # Sort by target
        if pd_before_17():
            score.sort([schema["target"], schema["reserved"]["score"]], axis=0, ascending=False, inplace=True)
        else:
            score.sort_values([schema["target"], schema["reserved"]["score"]], axis=0, ascending=False, inplace=True)
        theoreticalBestCounter = 0
        theoreticalBestArea = 0
        for i in xrange(rows):
            theoreticalBestCounter += score.iloc[i][1]
            theoreticalBestArea += theoreticalBestCounter

        # Sort by score
        if pd_before_17():
            score.sort([schema["reserved"]["score"], schema["target"]], axis=0, ascending=False, inplace=True)
        else:
            score.sort_values([schema["reserved"]["score"], schema["target"]], axis=0, ascending=False, inplace=True)
        weightedEventDict = {k : np.mean(map(lambda x: x[1], rows)) for k, rows in itertools.groupby(score.as_matrix(), lambda x: x[0])}

        actualBestCounter = 0
        actualBestArea = 0
        for i in xrange(rows):
            actualBestCounter += weightedEventDict[score.iloc[i][0]]
            actualBestArea += actualBestCounter

        if theoreticalBestArea != 0:
            self.logger.info("Actual best area = %f" % actualBestArea)
            self.logger.info("Theoretical best area = %f" % theoreticalBestArea)
            self.result = actualBestArea / float(theoreticalBestArea)
        else:
            self.logger.warn("All events are 0, could not calculate ROC score.")
            self.result = None

        # Add Result to Mediator
        self.mediator.rocscore = self.result
