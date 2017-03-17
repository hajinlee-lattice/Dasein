import simulatehadoop
from testbase import TestBase
from leframework.model.mediator import Mediator
import pandas as pd


class ScoreTargetBase(TestBase):

    def loadMediator(self, generator, scoreTarget):
        mediator = Mediator()
        mediator.schema = dict()
        mediator.schema["target"] = "Target"
        mediator.schema["reserved"] = dict()
        mediator.schema["reserved"]["score"] = "Score"
        mediator.data = pd.DataFrame(scoreTarget, columns=["Score", "Target"])
        mediator.allDataPreTransform = pd.DataFrame(scoreTarget, columns=["Score", "Target"])
        mediator.algorithmProperties = { "calibration_width" : 1.0 }
        generator.setMediator(mediator)
