from __future__ import print_function
from array import array
import imp
import json

from trainingtestbase import TrainingTestBase
import columntransform


class ConversionRateCategoricalColumnTransformTest(TrainingTestBase):

    def testPipelineColumnTransform(self):
        pipelineFilePath = ["../../main/python/configurablepipelinetransformsfromfile/pipeline.json".lower()]
        colTransform = columntransform.ColumnTransform(pathToPipelineFiles=pipelineFilePath)
        pipeline = colTransform.buildPipelineFromFile()[1]

        self.assertThatEachMemberOfPipelineHasTransformMethod(pipeline)
        self.checkThatTransformsDontThrowExceptions()
        self.assertNamedParameterListStatic()

        pipelineFilePath = ["../../main/python/configurablepipelinetransformsfromfile/evpipeline.json".lower()]
        colTransform = columntransform.ColumnTransform(pathToPipelineFiles=pipelineFilePath)
        pipeline = colTransform.buildPipelineFromFile()[1]

        self.assertThatEachMemberOfPipelineHasTransformMethod(pipeline)
        self.checkThatEVTransformsDontThrowExceptions()
        self.assertNamedParameterListStatic()

    def checkThatEVTransformsDontThrowExceptions(self):
        keys = ["highnumberuniquevaluesremovalstep", "revenuecolumntransformstep", "pivotstep", "imputationstepevpipeline", "columntypeconversionstep", "enumeratedcolumntransformstep", "cleancategoricalcolumn"
                , "assignconversionratetoallcategoricalvalues", "cleancategoricalcolumn"]
        pipelineFilePath = ["../../main/python/configurablepipelinetransformsfromfile/evpipeline.json".lower()]
        colTransform = columntransform.ColumnTransform(pathToPipelineFiles=pipelineFilePath)

        with open(pipelineFilePath[0]) as pipelineFile:
            pipelineFileAsJSON = json.load(pipelineFile)
            for _, value in pipelineFileAsJSON[colTransform.columnTransformKey].iteritems():
                self.assertTrue(value["Name"] in keys, "Couldn't find expected transform name %s in file" % value["Name"])

                uniqueColumnTransformName = value["Name"]
                columnTransformObject = {}

                try:
                    columnTransformObject["LoadedModule"] = imp.load_source(uniqueColumnTransformName,
                                                                            "./lepipeline.tar.gz/" + value["ColumnTransformFilePath"])
                except Exception as e:
                    print("Caught Exception:", e, "while RUNNING class initialization from pipeline")
                    raise

                mainClassName = value["MainClassName"]
                args = []
                namedParameterList = value["NamedParameterListToInit"]
                kwargs = colTransform.buildKwArgs(namedParameterList=namedParameterList, stringColumns=None, categoricalColumns=None, continuousColumns=None, targetColumn=None, columnsToTransform=None)
                self.aTestBuildKwArgs(namedParameterList)

                try:
                    getattr(columnTransformObject["LoadedModule"], mainClassName)(*args, **kwargs)
                except Exception as e:
                    print("Caught Exception:", e, "while RUNNING transform from pipeline")
                    raise

    def assertThatEachMemberOfPipelineHasTransformMethod(self, pipeline):
        for step in pipeline:
            self.assertTrue(hasattr(step, "transform"), "Each transform in pipeline should have a transform method")

    def checkThatTransformsDontThrowExceptions(self):
        keys = ["addtitleattributesstep", "addcompanyattributesstep", "addemailattributesstep", "addphoneattributesstep", "addnameattributesstep", "exportdfstep", "pivotstep", "imputationstep", "columntypeconversionstep", "enumeratedcolumntransformstep", "cleancategoricalcolumn"
                , "assignconversionratetocategoricalcolumns", "cleancategoricalcolumn", "remediatedatarulesstep", "assignconversionratetoallcategoricalvalues",
                "featureselectionstep", "unmatchedselectionstep", "highnumberuniquevaluesremovalstep", "categoricalgroupingstep"]
        pipelineFilePath = ["../../main/python/configurablepipelinetransformsfromfile/pipeline.json".lower()]
        colTransform = columntransform.ColumnTransform(pathToPipelineFiles=pipelineFilePath)

        with open(pipelineFilePath[0]) as pipelineFile:
            pipelineFileAsJSON = json.load(pipelineFile)
            for _, value in pipelineFileAsJSON[colTransform.columnTransformKey].iteritems():
                print(value["Name"])
                self.assertTrue(value["Name"] in keys, "Couldn't find expected transform name %s in file" % value["Name"])

                uniqueColumnTransformName = value["Name"]
                columnTransformObject = {}

                if "LoadFromHdfs" not in value or value["LoadFromHdfs"] == False:
                    try:
                        columnTransformObject["LoadedModule"] = imp.load_source(uniqueColumnTransformName,
                                                                                "./lepipeline.tar.gz/" + value["ColumnTransformFilePath"])
                    except Exception as e:
                        print("Caught Exception:", e, "while RUNNING class initialization from pipeline")
                        raise
                else:
                    columnTransformObject["LoadedModule"] = None

                mainClassName = value["MainClassName"]
                args = []
                namedParameterList = value["NamedParameterListToInit"]
                kwargs = colTransform.buildKwArgs(namedParameterList=namedParameterList, stringColumns=None, categoricalColumns=None, continuousColumns=None, targetColumn=None, columnsToTransform=None)
                if kwargs.has_key("enabled"):
                    del kwargs["enabled"]
                self.aTestBuildKwArgs(namedParameterList)
                try:
                    getattr(columnTransformObject["LoadedModule"], mainClassName)(*args, **kwargs)
                except Exception as e:
                    print("Caught Exception:", e, "while RUNNING transform from pipeline")
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