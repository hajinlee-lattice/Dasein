import sys
import pickle
from trainingtestbase import TrainingTestBase

class ParallelLearningExecutorTest(TrainingTestBase):

    def testExecuteLearning(self):
        # Import launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher

        traininglauncher = Launcher("model-parallel-driver.json")
        traininglauncher.execute(False)
        # Retrieve the pickled model from local directory
        with open("results/model.p", 'rb') as handle:
            pickleModel = pickle.load(handle)
            self.assertEqual(len(pickleModel), 1, "Wrong number of classifiers")
        