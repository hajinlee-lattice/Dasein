import logging

from collections import OrderedDict

from leframework.codestyle import overrides
from leframework.model.state import State


class ScoreDerivationGeneratorBase(State):
    def __init__(self, name):
        State.__init__(self, name)

    def _make_bucket(self, name, lower, upper):
        bucket = OrderedDict()
        bucket["name"] = name
        bucket["lower"] = lower
        bucket["upper"] = upper

        return bucket


class ScoreDerivationGenerator(ScoreDerivationGeneratorBase):
    def __init__(self):
        ScoreDerivationGeneratorBase.__init__(self, "ScoreDerivationGenerator")
        self._logger = logging.getLogger(name="ScoreDerivationGenerator")
    
    @overrides(State)
    def execute(self):
        mediator = self.getMediator()
        
        structure = OrderedDict()
        structure["target"] = None
        structure["averageProbability"] = mediator.averageProbability

        structure["buckets"] = list()
        for entry in mediator.buckets[::-1]:
            structure["buckets"].append(self._make_bucket(
                entry["Name"], entry["Minimum"], entry["Maximum"]))

        structure["percentiles"] = list()
        for entry in mediator.percentileBuckets[::-1]:
            structure["percentiles"].append(self._make_bucket(
                entry["Percentile"], entry["MinimumScore"], entry["MaximumScore"]))

        self.getMediator().score_derivation = structure


class RevenueScoreDerivationGenerator(ScoreDerivationGeneratorBase):
    def __init__(self):
        ScoreDerivationGeneratorBase.__init__(self, "RevenueScoreDerivationGenerator")
        self._logger = logging.getLogger(name="RevenueScoreDerivationGenerator")

    @overrides(State)
    def execute(self):
        mediator = self.getMediator()
        if mediator.revenueColumn is None:
            return

        structure = OrderedDict()
        structure["target"] = None

        structure["percentiles"] = list()
        maxValue = 0.0
        for entry in mediator.revenuesegmentations[0]['Segments'][::-1]:
            minValue = maxValue
            maxValue = entry['Max']
            score = entry['Score']
            if score <= 5:
                structure["percentiles"].append(self._make_bucket(5, minValue, maxValue))
            elif score >= 99:
                structure["percentiles"].append(self._make_bucket(99, minValue, maxValue))
            else:
                structure["percentiles"].append(self._make_bucket(entry['Score'], minValue, maxValue))

        self.getMediator().revenue_score_derivation = structure


class EVScoreDerivationGenerator(ScoreDerivationGeneratorBase):
    def __init__(self):
        ScoreDerivationGeneratorBase.__init__(self, "EVScoreDerivationGenerator")
        self._logger = logging.getLogger(name="EVScoreDerivationGenerator")

    @overrides(State)
    def execute(self):
        mediator = self.getMediator()
        if mediator.revenueColumn is None:
            return

        structure = OrderedDict()
        structure["target"] = None

        structure["percentiles"] = list()
        maxValue = 0.0
        for entry in mediator.evsegmentations[0]['Segments'][::-1]:
            minValue = maxValue
            maxValue = entry['Max']
            score = entry['Score']
            if score <= 5:
                structure["percentiles"].append(self._make_bucket(5, minValue, maxValue))
            elif score >= 99:
                structure["percentiles"].append(self._make_bucket(99, minValue, maxValue))
            else:
                structure["percentiles"].append(self._make_bucket(entry['Score'], minValue, maxValue))

        self.getMediator().ev_score_derivation = structure
