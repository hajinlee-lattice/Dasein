import pandas as pd

from testbase import TestBase
from leframework.model.mediator import Mediator

class ScoreTargetBase(TestBase):

    def loadMediator(self, generator, scoreTarget):
        mediator = Mediator()
        mediator.schema = dict()
        mediator.schema["target"] = "Target"
        mediator.schema["reserved"] = dict()
        mediator.schema["reserved"]["score"] = "Score"
        mediator.data = pd.DataFrame(scoreTarget, columns=["Score", "Target"])
        generator.setMediator(mediator)
