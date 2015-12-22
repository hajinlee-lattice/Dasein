import logging

from leframework.codestyle import overrides
from leframework.model.state import State

from sklearn import cross_validation

class CrossValidationGenerator(State):

    def __init__(self):
        State.__init__(self, "CrossValidationGenerator")
        self.logger = logging.getLogger(name='crossvalidationmetrics')
    
    @overrides(State)
    def execute(self):
        mediator = self.getMediator()

        if "cross_validation" in mediator.algorithmProperties:
            crossValidatedModelMean, crossValidatedModelStd = self.getCrossValidationMetrics(mediator)

            self.mediator.crossValidatedModelMean = crossValidatedModelMean
            self.mediator.crossValidatedModelStd = crossValidatedModelStd

        else:
            self.mediator.crossValidatedModelMean = None
            self.mediator.crossValidatedModelStd = None

    def getCrossValidationMetrics(self, mediator):
        try:
            features = mediator.allDataPostTransform[mediator.schema["features"]]
            target = mediator.allDataPostTransform[mediator.schema["target"]]
            numberOfFolds = int(mediator.algorithmProperties["cross_validation"])
            classifier = mediator.clf

            if numberOfFolds < 0:
                numberOfFolds = 3

            meanOfModelAccuracy, stdDecOfModelAccuracy = self.getMeanAndStdOfClf(classifier, features, target, numberOfFolds)

            return meanOfModelAccuracy, stdDecOfModelAccuracy

        except Exception as e:
            self.logger.error("Error caught while calculating Cross Validation Metrics. Error: %s " % e)

            return None, None

    def getMeanAndStdOfClf(self, classifier, features, targets, numberOfFolds):
        try:
            scores = cross_validation.cross_val_score( classifier, features, targets, cv=numberOfFolds)

            crossValidatedMeanOfModelAccuracy = scores.mean()
            crossValidatedStdOfModelAccuracy = scores.std()

            return crossValidatedMeanOfModelAccuracy, crossValidatedStdOfModelAccuracy

        except Exception as e :
            self.logger.error("Error caught while calculating Mean/Std of Model Accuracy. Error: %s " % e)

            return None, None
