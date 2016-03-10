from array import array
import imp
import json

import columntransform
from trainingtestbase import TrainingTestBase

class ConversionRateCategoricalColumnTransformTest(TrainingTestBase):

    def testPipelineColumnTransform(self):
        pipelineFilePath = ["../../main/python/configurablepipelinetransformsfromfile/pipeline.json".lower()]
        colTransform = columntransform.ColumnTransform(pathToPipelineFiles= pipelineFilePath)
        pipeline = colTransform.buildPipelineFromFile()

        self.assertLengthOfPipeline(pipeline)
        self.assertThatEachMemberOfPipelineHasTransformMethod(pipeline)
        self.checkThatTransformsDontThrowExceptions()
        self.assertNamedParameterListStatic()
        self.assertSortingOfTransform(pipeline)
        
        pipelineFilePath = ["../../main/python/configurablepipelinetransformsfromfile/evpipeline.json".lower()]
        colTransform = columntransform.ColumnTransform(pathToPipelineFiles= pipelineFilePath)
        pipeline = colTransform.buildPipelineFromFile()

        self.assertLengthOfEVPipeline(pipeline)
        self.assertThatEachMemberOfPipelineHasTransformMethod(pipeline)
        self.checkThatEVTransformsDontThrowExceptions()
        self.assertNamedParameterListStatic()
        self.assertSortingOfEVTransform(pipeline)

    def assertLengthOfEVPipeline(self, pipeline):
        lenOfPipeline = len(pipeline)
        self.assertEqual(len(pipeline), 6, "Pipeline should have 8 members, each representing a transform. Got: " + str(lenOfPipeline))

    def assertLengthOfPipeline(self, pipeline):
        lenOfPipeline = len(pipeline)
        self.assertEqual(len(pipeline), 7, "Pipeline should have 7 members, each representing a transform. Got: " + str(lenOfPipeline))

    def checkThatEVTransformsDontThrowExceptions(self):
        keys = ["revenuecolumntransformstep", "pivotstep", "imputationstepevpipeline", "columntypeconversionstep", "enumeratedcolumntransformstep", "cleancategoricalcolumn"
                , "assignconversionratertocategoricalcolumns", "cleancategoricalcolumn"]
        pipelineFilePath = ["../../main/python/configurablepipelinetransformsfromfile/evpipeline.json".lower()]
        colTransform = columntransform.ColumnTransform(pathToPipelineFiles= pipelineFilePath)

        with open(pipelineFilePath[0]) as pipelineFile:
            pipelineFileAsJSON = json.load(pipelineFile)
            for _, value in pipelineFileAsJSON[colTransform.columnTransformKey].iteritems():
                self.assertTrue(value["Name"] in keys, "Couldn't find expected transform name %s in file" % value["Name"])

                uniqueColumnTransformName = value["Name"]
                columnTransformObject = {}

                try:
                    columnTransformObject["LoadedModule"] = imp.load_source(uniqueColumnTransformName, value["ColumnTransformFilePath"])
                except Exception as E:
                    print "Caught Exception:",E, "while RUNNING class initialization from pipeline"
                    raise

                mainClassName = value["MainClassName"]
                args = []
                namedParameterList = value["NamedParameterListToInit"]
                kwargs = colTransform.buildKwArgs(namedParameterList = namedParameterList, stringColumns = None, categoricalColumns=None, continuousColumns=None, targetColumn=None, columnsToTransform=None)
                self.aTestBuildKwArgs(namedParameterList)

                try:
                    getattr(columnTransformObject["LoadedModule"], mainClassName)(*args, **kwargs)
                except Exception as e:
                    print "Caught Exception:", e, "while RUNNING transform from pipeline"
                    raise

    def assertThatEachMemberOfPipelineHasTransformMethod(self, pipeline):
        for step in pipeline:
            self.assertTrue(hasattr(step, "transform"), "Each transform in pipeline should have a transform method")

    def checkThatTransformsDontThrowExceptions(self):
        keys = ["exportdfstep", "pivotstep", "imputationstep", "columntypeconversionstep", "enumeratedcolumntransformstep", "cleancategoricalcolumn"
                , "assignconversionratertocategoricalcolumns", "cleancategoricalcolumn"]
        pipelineFilePath = ["../../main/python/configurablepipelinetransformsfromfile/pipeline.json".lower()]
        colTransform = columntransform.ColumnTransform(pathToPipelineFiles= pipelineFilePath)

        with open(pipelineFilePath[0]) as pipelineFile:
            pipelineFileAsJSON = json.load(pipelineFile)
            for _, value in pipelineFileAsJSON[colTransform.columnTransformKey].iteritems():
                print value["Name"]
                self.assertTrue(value["Name"] in keys, "Couldn't find expected transform name %s in file" % value["Name"])

                uniqueColumnTransformName = value["Name"]
                columnTransformObject = {}

                try:
                    columnTransformObject["LoadedModule"] = imp.load_source(uniqueColumnTransformName, value["ColumnTransformFilePath"])
                except Exception as E:
                    print "Caught Exception:",E, "while RUNNING class initialization from pipeline"
                    raise

                mainClassName = value["MainClassName"]
                args = []
                namedParameterList = value["NamedParameterListToInit"]
                kwargs = colTransform.buildKwArgs(namedParameterList = namedParameterList, stringColumns = None, categoricalColumns=None, continuousColumns=None, targetColumn=None, columnsToTransform=None)
                self.aTestBuildKwArgs(namedParameterList)

                try:
                    getattr(columnTransformObject["LoadedModule"], mainClassName)(*args, **kwargs)
                except Exception as e:
                    print "Caught Exception:", e, "while RUNNING transform from pipeline"
                    raise

    def aTestBuildKwArgs(self, namedParameterListAsJSON):
        colTransform = columntransform.ColumnTransform()

        namedParameterList = colTransform.buildKwArgs(namedParameterListAsJSON)

        self.assertIsNotNone(namedParameterList, "Named Parameter List not populated correctly")
        self.assertEquals(len(namedParameterList.keys()), len(namedParameterListAsJSON), "Named Parameter List should have 3 members. Got" + str(len(namedParameterList.keys())))

        if "orderedDictContinuousColumns" in namedParameterListAsJSON:
            self.assertIsNone(namedParameterList["orderedDictContinuousColumns"], "OrderedDictContinuousolumn should be None")
        if "emptyDictionary" in namedParameterListAsJSON:
            self.assertIsTrue(isinstance(namedParameterList["emptyDictionary"], dict), "Couldn't create Empty Dictionary")
        if "emptyArray" in namedParameterListAsJSON:
            self.assertIsTrue(isinstance(namedParameterList["emptyArray"], array), "Couldn't create Empty Array")

    def assertNamedParameterListStatic(self):
        namedParameterListAsJSON = '{"orderedDictContinuousColumns": "orderedDictContinuousColumns" , "emptyDictionary": "emptyDictionary", "targetColumn": "targetColumn", "emptyList": "emptyList"}'

        colTransform = columntransform.ColumnTransform()
        namedParameterList = colTransform.buildKwArgs(json.loads(namedParameterListAsJSON))
        self.assertIsNotNone(namedParameterList, "Named Parameter List not populated correctly")
        self.assertEquals(len(namedParameterList.keys()), 4, "Named Parameter List should have 4 members")
        self.assertIsNone(namedParameterList["orderedDictContinuousColumns"], "OrderedDictContinuousolumn should be None")
        self.assertTrue(isinstance(namedParameterList["emptyDictionary"], dict), "Couldn't create Empty Dictionary")
        self.assertTrue(isinstance(namedParameterList["emptyList"], list), "Couldn't create Empty List")
        
    def assertSortingOfTransform(self, pipeline):
        for i, step in enumerate(pipeline):
            print i, step
            if i == 0:
                self.assertEquals(step.__class__.__name__ , "PivotStep")
            if i == 1:
                self.assertEquals(step.__class__.__name__ , "EnumeratedColumnTransformStep")
            if i == 2:
                self.assertEquals(step.__class__.__name__ , "ColumnTypeConversionStep")
            if i == 3:
                self.assertEquals(step.__class__.__name__ , "ImputationStep")
            if i == 5:
                self.assertEquals(step.__class__.__name__ , "CleanCategoricalColumn")
            if i == 6:
                self.assertEquals(step.__class__.__name__ , "AssignConversionRateToCategoricalColumns")

    def assertSortingOfEVTransform(self, pipeline):
        for i, step in enumerate(pipeline):
            if i == 0:
                self.assertEquals(step.__class__.__name__ , "EnumeratedColumnTransformStep")
            if i == 1:
                self.assertEquals(step.__class__.__name__ , "ColumnTypeConversionStep")
            if i == 2:
                self.assertEquals(step.__class__.__name__ , "RevenueColumnTransformStep")
            if i == 3:
                self.assertEquals(step.__class__.__name__ , "ImputationStep")
            if i == 4:
                self.assertEquals(step.__class__.__name__ , "CleanCategoricalColumn")
            if i == 5:
                self.assertEquals(step.__class__.__name__ , "AssignConversionRateToCategoricalColumns")
