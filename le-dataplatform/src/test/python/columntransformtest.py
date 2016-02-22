import columntransform

import json
import imp

from trainingtestbase import TrainingTestBase
from array import array

class ColumnTransformTest(TrainingTestBase):

    def testColumnTransform(self):
        pipelineFilePath = ["./configurablepipelinetransformsfromfile/pipeline.json".lower()]
        colTransform = columntransform.ColumnTransform(pathToPipelineFiles= pipelineFilePath)
        pipeline = colTransform.buildPipelineFromFile()

        self.assertLengthOfPipeline(pipeline)
        self.assertThatEachMemberOfPipelineHasTransformMethod(pipeline)
        self.checkThatTransformsDontThrowExceptions()
        self.assertNamedParameterListStatic()

    def assertLengthOfPipeline(self, pipeline):
        lenOfPipeline = len(pipeline)
        self.assertEqual(len(pipeline), 3, "Pipeline should have 4 members, each representing a transform. Got: " + str(lenOfPipeline))

    def assertThatEachMemberOfPipelineHasTransformMethod(self, pipeline):
        for step in pipeline:
            self.assertTrue(hasattr(step, "transform"), "Each transform in pipeline should have a transform method")

    def checkThatTransformsDontThrowExceptions(self):
        keys = ["imputationstep", "columntypeconversionstep", "enumeratedcolumntransformstep", "cleancategoricalcolumn"]
        pipelineFilePath = ["./configurablepipelinetransformsfromfile/pipeline.json".lower()]
        colTransform = columntransform.ColumnTransform(pathToPipelineFiles= pipelineFilePath)

        with open(pipelineFilePath[0]) as pipelineFile:
            pipelineFileAsJSON = json.load(pipelineFile)
            for attr, value in pipelineFileAsJSON[ colTransform.columnTransformKey ].iteritems():
                self.assertTrue(value["Name"] in keys, "Couldn't find expected transform name " + value["Name"] + " in file")

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
                kwargs = colTransform.buildKwArgs(namedParameterList = namedParameterList, StringColumns = None, CategoricalColumns=None, ContinuousColumns=None, targetColumn=None, ColumnsToTransform=None)
                self.aTestBuildKwArgs(namedParameterList)

                try:
                    loadedObject = getattr(columnTransformObject["LoadedModule"], mainClassName)(*args, **kwargs)
                except Exception as E:
                    print "Caught Exception:",E, "while RUNNING transform from pipeline"
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
