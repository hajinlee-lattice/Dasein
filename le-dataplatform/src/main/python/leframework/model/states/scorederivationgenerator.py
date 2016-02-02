import logging

from collections import OrderedDict

from leframework.codestyle import overrides
from leframework.model.state import State


class ScoreDerivationGenerator(State):
    def __init__(self):
        State.__init__(self, "ScoreDerivationGenerator")
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
    
    def _make_bucket(self, name, lower, upper):
        bucket = OrderedDict()
        bucket["name"] = name
        bucket["lower"] = lower
        bucket["upper"] = upper
        
        return bucket
    
