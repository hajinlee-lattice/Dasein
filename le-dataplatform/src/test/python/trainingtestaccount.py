import filecmp
import glob
import json
import os
import pickle
from random import random
from sklearn.ensemble import RandomForestClassifier
import sys

from leframework import scoringengine as se
from trainingtestbase import TrainingTestBase


class TrainingTestAccount(TrainingTestBase):

    def testExecuteLearningAccountModel1(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        traininglauncher = Launcher("metadata-modeling-account-model-1.json")
        traininglauncher.execute(False)

        with open('results/enhancements/modelsummary.json', mode='rb') as modelsummaryfile:
            modelsummary = json.loads(modelsummaryfile.read())
            topSample = modelsummary['TopSample']
            self.assertTrue('Website' in topSample[0])
            self.assertTrue('FirstName' not in topSample[0])
            self.assertTrue('LastName' not in topSample[0])
