import glob
import os
import pickle
import shutil
import sys

from trainingtestbase import TrainingTestBase


class ParallelRegressionLearningExecutorTest(TrainingTestBase):

    def setUp(self):
        super(ParallelRegressionLearningExecutorTest, self).setUp()
    
    def tearDown(self):
        super(ParallelRegressionLearningExecutorTest, self).tearDown()
        shutil.rmtree("./evpipeline.tar.gz", ignore_errors=True)


    def testExecuteLearning(self):
        # Import launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        traininglauncher = Launcher("modeldriver-regression-evpipeline.json")
        traininglauncher.execute(False)
        # Retrieve the pickled model from local directory
        with open("results/model.p", 'rb') as handle:
            pickleModel = pickle.load(handle)
            self.assertEqual(len(pickleModel), 2, "Wrong number of classifiers")
        