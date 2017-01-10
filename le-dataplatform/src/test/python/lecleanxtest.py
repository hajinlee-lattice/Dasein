import filecmp
import glob
import json
import pickle
from random import random
import sys

from leframework import scoringengine as se
from trainingtestbase import TrainingTestBase

class LECLEANXTest(TrainingTestBase):

    def testExecuteLearning(self):
        from aggregatedmodel import AggregatedModel
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        traininglauncher = Launcher("LECLEANX_metadata.json")
        traininglauncher.execute(False)
