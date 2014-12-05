import os
import shutil
from unittest import TestCase

from launcher import Launcher


class TrainingTest(TestCase):
    
    def setUp(self):
        if os.path.exists("./results"):
            shutil.rmtree("./results")
    
    def testExecute(self):
        l = Launcher("model.json")
        '''
          writeToHdfs = True writes any created artifacts to hdfs.
          validateEnv = True validates for the existence of particular env variables that YARN creates.
          postProcessClf = True initiates the state machine that generates the summary.
        '''
        l.execute(writeToHdfs=False, validateEnv=False, postProcessClf=True)
        clf = l.getClassifier()
        self.assertTrue(clf is not None)
