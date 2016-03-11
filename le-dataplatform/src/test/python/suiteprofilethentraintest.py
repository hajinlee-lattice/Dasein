import csv
import glob
import json
import math
import os
import subprocess
import sys

from leframework.executors.learningexecutor import LearningExecutor
from trainingtestbase import TrainingTestBase

class SuiteProfilingThenTrainTest(TrainingTestBase):
    def executeProfilingThenTrain(self):
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher
        
        # These properties won't really be used since these are just unit tests.
        # Functional and end-to-end tests should be done from java
        profilinglauncher = Launcher("metadata-profile.json")
        profilinglauncher.execute(False, postProcessClf=False)
        learningExecutor = LearningExecutor()
        results = learningExecutor.retrieveMetadata("./results/profile.avro", False)
        self.assertIsNotNone(results)
        
        # Dynamically import launcher to make sure globals() is clean in launcher
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        from launcher import Launcher
        os.symlink("./results/profile.avro", "profile.avro")
        os.symlink("../resources/com/latticeengines/dataplatform/python/modelpredictorextraction.py", "modelpredictorextraction.py")
        traininglauncher = Launcher("metadata-model.json")
        traininglauncher.execute(False)
        jsonDict = json.loads(open(glob.glob("./results/*PLSModel*.json")[0]).read())
        self.assertIsNotNone(jsonDict)
        self.assertModelOutput(results[0], jsonDict)
        
    def assertModelOutput(self, metadataInProfile, modelDict):
        self.assertTrue(modelDict.has_key("AverageProbability"))
        self.assertTrue(modelDict.has_key("Model"))
        self.assertTrue(modelDict.has_key("Name"))
        self.assertTrue(len(modelDict["Buckets"]) > 0)
        self.assertTrue(len(modelDict["InputColumnMetadata"]) > 0)
        
        self.assertTrue(modelDict["Summary"].has_key("SchemaVersion"))
        self.assertTrue(modelDict["Summary"].has_key("DLEventTableData"))
        self.assertTrue(modelDict["Summary"].has_key("ConstructionInfo"))
        self.assertTrue(len(modelDict["Summary"]["SegmentChart"]) > 0)
        self.assertTrue(len(modelDict["PercentileBuckets"]) > 0)
        self.assertTrue(len(modelDict["NormalizationBuckets"]) > 0)
        predictors = modelDict["Summary"]["Predictors"]
        self.assertTrue(len(predictors) > 0)
        
        columnsInPredictors = set()
        columnsInProfile = set(metadataInProfile.keys())
        for predictor in predictors:
            columnsInPredictors.add(predictor["Name"])
        self.assertEqual(columnsInProfile, columnsInPredictors)
        
        configMetadataFile = "metadata.avsc"
        configMetadata = json.loads(open(configMetadataFile, "rb").read())["Metadata"]
        columnsInMetadata = set()
        for c in configMetadata:
            u = c["ApprovedUsage"]
            if isinstance(u, list) and u[0] == "None":
                columnsInMetadata.add(c["ColumnName"])
        self.assertEqual(len(columnsInMetadata.intersection(columnsInPredictors)), 0)
                         
    def tearDown(self):
        super(TrainingTestBase, self).tearDown()
        # Remove launcher module to restore its globals()
        if 'launcher' in sys.modules:
            del sys.modules['launcher']
        
class SuiteMuleSoftProfilingThenTrainTest(SuiteProfilingThenTrainTest):
    def testExecuteProfilingThenTrain(self):
        super(SuiteMuleSoftProfilingThenTrainTest, self).executeProfilingThenTrain()
        jsonDict = json.loads(open(glob.glob("./results/*PLSModel*.json")[0]).read())
        rocScore = jsonDict["Summary"]["RocScore"]
        self.assertTrue(rocScore > 0.7)

    @classmethod
    def getSubDir(cls):
        return "Mulesoft_Relaunch"

class SuiteHirevueProfilingThenTrainTest(SuiteProfilingThenTrainTest):
    def testExecuteProfilingThenTrain(self):
        super(SuiteHirevueProfilingThenTrainTest, self).executeProfilingThenTrain()
        jsonDict = json.loads(open(glob.glob("./results/*PLSModel*.json")[0]).read())
        rocScore = jsonDict["Summary"]["RocScore"]
        self.assertTrue(rocScore > 0.7)

    @classmethod
    def getSubDir(cls):
        return "PLS132_test_Hirevue"

class SuiteLatticeRelaunchProfilingThenTrainTest(SuiteProfilingThenTrainTest):
    def testExecuteProfilingThenTrain(self):
        super(SuiteLatticeRelaunchProfilingThenTrainTest, self).executeProfilingThenTrain()
        jsonDict = json.loads(open(glob.glob("./results/*PLSModel*.json")[0]).read())
        rocScore = jsonDict["Summary"]["RocScore"]
        self.assertTrue(rocScore > 0.7)

        self.createCSVFromModel("metadata-model.json", "./results/scorefilefortrainingdata.txt", True)
        for index in range(0, len(jsonDict["Model"]["CompressedSupportFiles"])):
            entry = jsonDict["Model"]["CompressedSupportFiles"][index]
            fileName = "./results/" + entry["Key"] + ".gz"
            self.decodeBase64ThenDecompressToFile(entry["Value"], fileName)
            os.rename(fileName, "./results/" + entry["Key"])

        with open("./results/scoringengine.py", "w") as scoringScript:
            scoringScript.write(jsonDict["Model"]["Script"])


        with open("./results/scorefileforonerow.txt", "w") as fw:
            with open("./results/scorefilefortrainingdata.txt", "r") as fr:
                for line in fr:
                    fw.write(line)
                    break

        predictors = jsonDict["Summary"]["Predictors"]
        oneRowData = json.loads(open("./results/scorefileforonerow.txt", "r").read())
        for data in oneRowData["value"]:
            # Modify top 5 predictors
            for i in range(0, 9):
                if data["Key"] == predictors[i]["Name"]:
                    typeAndValue = data["Value"]["SerializedValueAndType"].split("|")
                    valueType = typeAndValue[0]
                    value = 1.0
                    if len(typeAndValue) == 2:
                        value = float(typeAndValue[1][1:-1])
                    if valueType == "Float":
                        data["Value"]["SerializedValueAndType"] = "%s|'%f'" % (valueType, value * 100)
                    elif valueType == "String":
                        data["Value"]["SerializedValueAndType"] = "%s|'SomeRandomString'" % valueType
        with open("./results/scorefileforonerowchanged.txt", "w") as fp:
            json.dump(oneRowData, fp)
        os.environ["PYTHONPATH"] = ''
        subprocess.call([sys.executable, './results/scoringengine.py', "./results/scorefileforonerow.txt", "./results/scorefileforonerow.output.txt"])
        subprocess.call([sys.executable, './results/scoringengine.py', "./results/scorefileforonerowchanged.txt", "./results/scorefileforonerowchanged.output.txt"])
        score1 = float(csv.reader(open("./results/scorefileforonerow.output.txt", "r")).next()[1])
        score2 = float(csv.reader(open("./results/scorefileforonerowchanged.output.txt", "r")).next()[1])

        self.assertTrue(math.fabs(score1 - score2) > 0.001)


        subprocess.call([sys.executable, './results/scoringengine.py', "./results/scorefilefortrainingdata.txt", "./results/scoreoutputfilefortrainingdata.txt"])
        self.__writePercentileBucketsFile("./results/scoreoutputfilefortrainingdata.txt", "./results/pctilescoreoutputfilefortrainingdata.csv", jsonDict)
        self.__writePercentileBucketsFile(glob.glob("./results/*scored.txt")[0], "./results/pctilescoreoutputfilefortestdata.csv", jsonDict)

    def __writePercentileBucketsFile(self, inputFileName, outputFileName, jsonDict):
        with open(outputFileName, "w") as fw:
            csvwriter = csv.writer(fw)
            with open(inputFileName, "r") as csvreader:
                for row in csv.reader(csvreader):
                    rawScore = float(row[1])
                    csvwriter.writerow((row[0], rawScore, self.__getPercentileScore(jsonDict["PercentileBuckets"], rawScore)))

    def __getPercentileScore(self, percentileBuckets, rawScore):
        for percentileBucket in percentileBuckets:
            minScore = percentileBucket["MinimumScore"]
            maxScore = percentileBucket["MaximumScore"]
            percentile = percentileBucket["Percentile"]

            if percentile == 100 and rawScore >= maxScore:
                return 100
            if percentile == 1 and rawScore <= minScore:
                return 1
            elif rawScore > minScore and rawScore < maxScore:
                return percentile

    @classmethod
    def getSubDir(cls):
        return "Lattice_Relaunch"

class SuiteDocsignProfilingThenTrainTest(SuiteProfilingThenTrainTest):
    def testExecuteProfilingThenTrain(self):
        super(SuiteDocsignProfilingThenTrainTest, self).executeProfilingThenTrain()
        jsonDict = json.loads(open(glob.glob("./results/*PLSModel*.json")[0]).read())
        rocScore = jsonDict["Summary"]["RocScore"]
        predictors = jsonDict["Summary"]["Predictors"]
        for predictor in predictors:
            if predictor['Name'] == 'AssetsStartOfYear':
                startOfYearPredictor = predictor
        self.assertIsNotNone(startOfYearPredictor)
        self.assertEqual(startOfYearPredictor['FundamentalType'], "year")
        self.assertTrue(rocScore > 0.3)

        count = 0
        hasOther = False
        binarySet = set()
        csvFile = open(glob.glob("./results/*PLSModel*_model.csv")[0])
        csvDictReader = csv.DictReader(csvFile)
        for row in csvDictReader:
            if row['Attribute Name'] == 'ELQContact_Industry':
                if row['Attribute Value'] == '["Other"]':
                    hasOther = True
                else:
                    self.assertTrue(float(row['Frequency/Total Leads']) >= 0.01)
                count += 1
            if row['Attribute Name'] == 'Open Source Adoption':
                binarySet.add(row['Attribute Value'])

        from modelpredictorextraction import columnNames
        csvFile.seek(0);
        predictorElementRow = csvDictReader.next()
        for columnName in predictorElementRow:
            if columnName in columnNames:
                columnNames.remove(columnName)
        self.assertTrue(len(columnNames) == 0)

        self.assertTrue(count == 9)
        self.assertTrue(hasOther)

        self.assertEqual(len(binarySet), 3)
        self.assertTrue(binarySet == set(['["Yes"]', '["No"]', '["Not Available"]']))

    @classmethod
    def getSubDir(cls):
        return "PLS132_test_Docusign"
    
class SuiteTenant1ProfilingThenTrainTest(SuiteProfilingThenTrainTest):
    def testExecuteProfilingThenTrain(self):
        super(SuiteTenant1ProfilingThenTrainTest, self).executeProfilingThenTrain()
        jsonDict = json.loads(open(glob.glob("./results/*PLSModel*.json")[0]).read())
        rocScore = jsonDict["Summary"]["RocScore"]
        self.assertTrue(rocScore > 0.5)

        # Summary File Exists?
        summaryFile = "./results/enhancements/modelsummary.json"
        self.assertTrue(os.path.isfile(summaryFile))

        # Load Summary
        summary = json.loads(open(summaryFile).read())

        # Check Top Sample
        topSample = summary["TopSample"]
        self.assertEqual(len(topSample), 10)
        self.assertEqual(reduce(lambda acc, e: acc + (1 if e["Converted"] else 0), topSample, 0), 7)
        self.assertEqual(reduce(lambda acc, e: acc + (0 if e["Converted"] else 1), topSample, 0), 3)
        self.assertEqual(reduce(lambda acc, e: acc + (1 if e["FirstName"] is None else 0), topSample, 0), 0)
        self.assertEqual(reduce(lambda acc, e: acc + (1 if len(e["FirstName"]) == 0 else 0), topSample, 0), 0)
        self.assertEqual(reduce(lambda acc, e: acc + (1 if e["LastName"] is None else 0), topSample, 0), 0)
        self.assertEqual(reduce(lambda acc, e: acc + (1 if len(e["LastName"]) == 0 else 0), topSample, 0), 0)
        self.assertEqual(reduce(lambda acc, e: acc + (1 if e["Company"] is None else 0), topSample, 0), 0)
        self.assertEqual(reduce(lambda acc, e: acc + (1 if len(e["Company"]) == 0 else 0), topSample, 0), 0)
        self.assertEqual(reduce(lambda acc, e: acc + (0 if isinstance(e["Score"], int) else 1), topSample, 0), 0)
        self.assertEqual(len(set([e["Company"] for e in topSample])), 10)

        # Check Bottom Sample
        bottomSample = summary["BottomSample"]
        self.assertEqual(len(bottomSample), 10)
        self.assertEqual(reduce(lambda acc, e: acc + (1 if e["Converted"] else 0), bottomSample, 0), 0)
        self.assertEqual(reduce(lambda acc, e: acc + (1 if e["FirstName"] is None else 0), bottomSample, 0), 0)
        self.assertEqual(reduce(lambda acc, e: acc + (1 if len(e["FirstName"]) == 0 else 0), bottomSample, 0), 0)
        self.assertEqual(reduce(lambda acc, e: acc + (1 if e["LastName"] is None else 0), bottomSample, 0), 0)
        self.assertEqual(reduce(lambda acc, e: acc + (1 if len(e["LastName"]) == 0 else 0), bottomSample, 0), 0)
        self.assertEqual(reduce(lambda acc, e: acc + (1 if e["Company"] is None else 0), bottomSample, 0), 0)
        self.assertEqual(reduce(lambda acc, e: acc + (1 if len(e["Company"]) == 0 else 0), bottomSample, 0), 0)
        self.assertEqual(reduce(lambda acc, e: acc + (0 if isinstance(e["Score"], int) else 1), bottomSample, 0), 0)
        self.assertEqual(len(set([e["Company"] for e in bottomSample])), 10)
        
        #Check for Model details
        templateVersion = summary["ModelDetails"]["TemplateVersion"]
        self.assertEqual(templateVersion, "v1.0")

    @classmethod
    def getSubDir(cls):
        return "Tenant1"
    
class SuiteTenant2ProfilingThenTrainTest(SuiteProfilingThenTrainTest):
    def testExecuteProfilingThenTrain(self):
        super(SuiteTenant2ProfilingThenTrainTest, self).executeProfilingThenTrain()
        jsonDict = json.loads(open(glob.glob("./results/*PLSModel*.json")[0]).read())
        rocScore = jsonDict["Summary"]["RocScore"]
        self.assertTrue(rocScore > 0.5)


    @classmethod
    def getSubDir(cls):
        return "Tenant2"
