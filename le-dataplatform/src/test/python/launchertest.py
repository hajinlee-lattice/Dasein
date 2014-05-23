import launcher as l
import os
import shutil
import unittest

class LauncherTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        shutil.rmtree("results")

    def testExecute(self):
        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        os.environ["CONTAINER_ID"] = "xyz"
        os.environ["SHDP_HD_FSWEB"] = "localhost:50070"
        launcher = l.Launcher("model.json")
        launcher.execute(False)
        
