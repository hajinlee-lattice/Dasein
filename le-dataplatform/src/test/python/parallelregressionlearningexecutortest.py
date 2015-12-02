import sys
import os
import shutil
import pickle
from trainingtestbase import TrainingTestBase

class ParallelRegressionLearningExecutorTest(TrainingTestBase):

    def setUp(self):
        super(ParallelRegressionLearningExecutorTest, self).setUp()
        shutil.rmtree("./evpipeline.tar.gz", ignore_errors=True)
        os.makedirs("./evpipeline.tar.gz")
        shutil.copy("../../main/python/evpipeline/evpipelinesteps.py", "./evpipeline.tar.gz/evpipelinesteps.py")
        shutil.copy("../../main/python/pipeline/encoder.py", "./evpipeline.tar.gz/encoder.py")
        os.symlink("../../main/python/evpipeline/evpipeline.py", "evpipeline.py")
        sys.path.append("./evpipeline.tar.gz")
    
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
        