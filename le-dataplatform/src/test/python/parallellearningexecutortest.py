import sys
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
        modelPickle = open("results/model.p")
        self.assertTrue(modelPickle is not None, "Model pickle is not generated.")
        