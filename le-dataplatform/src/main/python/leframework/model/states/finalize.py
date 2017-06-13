from collections import OrderedDict
import csv
import json
import logging
import os
import subprocess
import sys

import numpy

from leframework.codestyle import overrides
from leframework.model.jsongenbase import JsonGenBase
from leframework.model.state import State


class Finalize(State):

    def __init__(self):
        State.__init__(self, "Finalize")
        self.logger = logging.getLogger(name='finalize')

    @overrides(State)
    def execute(self):
        self.writeJson(self.getMediator())
        self.writeScoredText(self.getMediator())
        self.writeRevenueStatisticsText(self.getMediator())
        self.invokeModelPredictorsExtraction(self.getMediator())
        self.writeReadoutSample(self.getMediator())
        self.writeEnhancedFiles(self.getMediator())
        self.writeMessages(self.getMediator())
        self.writeExportData(self.getMediator())
        self.writePipelineDebugArtifacts(self.getMediator())
        self.writeModelQualityFile(self.getMediator())

    def writeMessages(self, mediator):
        if len(mediator.messages) > 0:
            with open(mediator.modelLocalDir + mediator.name + "_messages.txt", "wb") as fp:
                fp.write("\n".join(mediator.messages))

    def writeScoredText(self, mediator):
        scored = mediator.data[mediator.schema["reserved"]["score"]].as_matrix()
        # add the key data and append the scored data
        keyData = mediator.data[mediator.schema["keys"]].as_matrix().astype(str)
        eventData = mediator.data[mediator.schema["target"]].as_matrix().astype(str)
        # write the scored data to file
        with open(mediator.modelLocalDir + mediator.name + "_scored.txt", 'wb') as scoredFile:
            scoredWriter = csv.writer(scoredFile)
            for i in range(0,keyData.size):
                scoredWriter.writerow([keyData[i][0], '{:.6f}'.format(scored[i])])
        # write the target data to file
        numpy.savetxt(mediator.modelLocalDir + mediator.name + "_target.txt", eventData, delimiter=",", fmt="%s")

    def writeRevenueStatisticsText(self, mediator):
        if (mediator.revenueColumn is not None and mediator.revenueStatistics is not None):
            numpy.savetxt(mediator.modelLocalDir + mediator.name + "_revenue_statistics.txt", mediator.revenueStatistics, delimiter=",", fmt="%s")

    def writeJson(self, mediator):
        stateMachine = self.getStateMachine()
        states = stateMachine.getStates()

        jsonDict = OrderedDict()
        for state in states:
            if isinstance(state, JsonGenBase):
                key = state.getKey()
                value = state.getJsonProperty()
                jsonDict[key] = value

        with open(mediator.modelLocalDir + mediator.name + "_model.json", "wb") as fp:
            json.dump(jsonDict, fp)

    def invokeModelPredictorsExtraction(self, mediator):
        modelPredictor = mediator.modelPredictor
        with open(mediator.modelLocalDir + mediator.name + "_model.csv", 'wb') as modelPredictorFile:
            modelPredictorWriter = csv.DictWriter(modelPredictorFile, fieldnames=modelPredictor['colnames'])
            modelPredictorWriter.writeheader()
            for row in modelPredictor['rows']:
                rowmap = {}
                i = 0
                for col in modelPredictor['colnames']:
                    rowmap[col] = row[i]
                    i += 1
                modelPredictorWriter.writerow(rowmap)

    def writeReadoutSample(self, mediator):
        csvFilePath = mediator.modelLocalDir + mediator.name + "_readoutsample.csv"
        self.mediator.readoutsample.to_csv(csvFilePath, index=False)

    def writeExportData(self, mediator):
        exportFilePath = mediator.modelLocalDir + mediator.name + "_dataexport.csv"
        if os.path.isfile("./exportdfstep.csv"):
            os.rename("./exportdfstep.csv", exportFilePath)
        exportFilePath = mediator.modelLocalDir + mediator.name + "_exportrftrain.csv"
        if os.path.isfile("./exportrftrain.csv"):
            os.rename("./exportrftrain.csv", exportFilePath)

    def writePipelineDebugArtifacts(self, mediator):
        base = self.mediator.pipelineLocalDir
        for step in mediator.scoringPipeline.getPipeline():
            for item in step.getDebugArtifacts():
                fileName, value = item.iteritems().next()
                with open(os.path.join(base, fileName), "wb") as f:
                    json.dump(value, f, indent=4)

    def writeEnhancedFiles(self, mediator):
        base = self.mediator.modelEnhancementsLocalDir

        with open(os.path.join(base, "modelsummary.json"), "wb") as f:
            json.dump(self.mediator.enhancedsummary, f, indent=4)

        with open(os.path.join(base, "datacomposition.json"), "wb") as f:
            json.dump(self.mediator.data_composition, f, indent=4)

        with open(os.path.join(base, "scorederivation.json"), "wb") as f:
            json.dump(self.mediator.score_derivation, f, indent=4)

    def writeModelQualityFile(self, mediator):
        base = self.mediator.modelEnhancementsLocalDir

        try:
            if hasattr(self.mediator, "modelquality"):
                with open(os.path.join(base, "revenuemodelqualitymetrics.json"), "wb") as f:
                    json.dump(self.mediator.modelquality, f, indent=4)
        except:
            self.logger.error("Error while trying to write model quality file")
