import os
import shutil
import sys
from trainingtestbase import TrainingTestBase

class ProfilingTestBase(TrainingTestBase):

    def setUp(self):
        super(ProfilingTestBase, self).setUp()

        # Symbolic links will be cleaned up by testBase
        results = "./results"
        if os.path.exists(results):
            shutil.rmtree(results)

        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']

    def tearDown(self):
        super(ProfilingTestBase, self).tearDown()
        # Remove launcher module to restore its globals()
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
