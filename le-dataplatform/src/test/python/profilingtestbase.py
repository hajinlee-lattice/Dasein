import os
import shutil
import sys
from trainingtestbase import TrainingTestBase

class ProfilingTestBase(TrainingTestBase):

    def setUp(self):
        super(ProfilingTestBase, self).setUp()
        script = "data_profile.py"
        if os.path.exists(script):
            os.remove(script)

        # Symbolic links will be cleaned up by testBase
        os.symlink("../../main/python/algorithm/" + script, script)
        results = "./results"
        if os.path.exists(results):
            shutil.rmtree(results)
            
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']

    def tearDown(self):
        super(ProfilingTestBase, self).tearDown()
        # Remove launcher module to restore its globals()
        del sys.modules['launcher']

