from collections import OrderedDict
import logging

from leframework.codestyle import overrides
from leframework.model.state import State


class EnhancedSummaryGenerator(State):

    def __init__(self):
        State.__init__(self, "EnhancedSummaryGenerator")
        self.logger = logging.getLogger(name='enhancedsummarygenerator')

    @overrides(State)
    def execute(self):
        self.result = OrderedDict()

        if hasattr(self.mediator, "segmentations"):
            self.result["Segmentations"] = self.mediator.segmentations
        
        if hasattr(self.mediator, "predictors"):
            self.result["Predictors"] = self.mediator.predictors
        
        if hasattr(self.mediator, "modeldetails"):
            self.result["ModelDetails"] = self.mediator.modeldetails
            
        if hasattr(self.mediator, "topsample"):
            self.result["TopSample"] = self.mediator.topsample
        
        if hasattr(self.mediator, "bottomsample"):
            self.result["BottomSample"] = self.mediator.bottomsample
        
        if hasattr(self.mediator, "eventtableprovenance"):
            self.result["EventTableProvenance"] = self.mediator.eventtableprovenance

        if "cross_validation" in self.mediator.algorithmProperties:
            self.result["CrossValidatedMeanOfModelAccuracy"] = self.mediator.crossValidatedModelMean
            self.result["CrossValidatedStdOfModelAccuracy"] = self.mediator.crossValidatedModelStd

        # Add Result to Mediator
        self.mediator.enhancedsummary = self.result
