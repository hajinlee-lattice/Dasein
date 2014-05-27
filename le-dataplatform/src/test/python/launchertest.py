import json
import os
import shutil
from sklearn.externals import joblib
from sklearn.linear_model import LogisticRegression
from unittest import TestCase

from launcher import Launcher


class LauncherTest(TestCase):

    @classmethod
    def setUpClass(cls):
        if os.path.exists("./results"):
            shutil.rmtree("./results")


    def testExecute(self):
        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        os.environ["CONTAINER_ID"] = "xyz"
        os.environ["SHDP_HD_FSWEB"] = "localhost:50070"
        launcher = Launcher("model.json")
        launcher.execute(False)
        
        # Retrieve the pickled model from the json file
        jsonDict = json.loads(open("./results/model.json").read())
        pklByteArray = bytearray(jsonDict["Model"]["SupportFiles"][0]["Value"])
        # Write to the file system
        with open("./results/readFromJsonModel.pkl", "wb") as output:
            output.write(pklByteArray)
        
        # Load from the file system and deserialize into the model
        clf = joblib.load("./results/readFromJsonModel.pkl")
        
        self.assertTrue(isinstance(clf, LogisticRegression), "clf not instance of sklearn LogisticRegression.")
        
