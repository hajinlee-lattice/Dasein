'''
Description:

    This step will take top N features and remove it from the data frame
'''
from sklearn import ensemble

from pipelinefwk import PipelineStep
from pipelinefwk import get_logger
import random

logger = get_logger("pipeline")

class FeatureSelectionStep(PipelineStep):

    params = {}
    numFeatures = 50

    def __init__(self, params, numFeatures=50):
        self.params = params
        self.numFeatures = numFeatures

    def transform(self, dataFrame, configMetadata, test):
        if test:
            return dataFrame
        logger.info("Doing feature selection.")
        clf = ensemble.RandomForestClassifier(criterion="gini",
                                          n_estimators=100,
                                          min_samples_split=25,
                                          min_samples_leaf=10,
                                          max_depth=5,
                                          max_features=None,
                                          random_state=123456,
                                          bootstrap=True)
        mediator = self.getMediator()
        features = self.params["schema"]["original_features"]
        removed = set()
        
        if "REMOVEDCOLUMNS" in mediator:
            removed = set(mediator["REMOVEDCOLUMNS"])
        features = [x for x in  features if x not in removed]
        
        if "ADDEDCOLUMNS" in mediator:
            features.extend([x["ColumnName"] for x in mediator["ADDEDCOLUMNS"]])
        sampleFrame = dataFrame
        if dataFrame.shape[0] > 100000:
            logger.info("Large file, doing sampling, record#=" + str(dataFrame.shape[0]))
            rows = random.sample(dataFrame.index, int(dataFrame.shape[0] * 0.1))
            sampleFrame = dataFrame.iloc[rows]
        X_train = sampleFrame[features]
        Y_train = sampleFrame[self.params["schema"]["target"]]
        clf.fit(X_train, Y_train)
        importances = clf.feature_importances_
        numInputs = len(importances)
        
        f = {}
        for i in range(0, numInputs):
            f[features[i]] = importances[i]
        features = sorted(f.items(), key=lambda x: x[1], reverse=True)
        features = [x for x, _ in features]
        
        super(FeatureSelectionStep, self).removeColumns(dataFrame, features[self.numFeatures:])
        return dataFrame

    def getOutputColumns(self):
        return []

    def getRTSMainModule(self):
        return ""

    def getRTSArtifacts(self):
        return []

    def doColumnCheck(self):
        return False

    def getDebugArtifacts(self):
        return []

    def includeInScoringPipeline(self):
        return False
