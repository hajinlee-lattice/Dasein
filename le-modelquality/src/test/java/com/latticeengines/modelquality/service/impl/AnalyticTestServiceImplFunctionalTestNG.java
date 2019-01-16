package com.latticeengines.modelquality.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.AnalyticTestEntityNames;
import com.latticeengines.domain.exposed.modelquality.AnalyticTestType;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.DataSetType;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.ScoringDataSet;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class AnalyticTestServiceImplFunctionalTestNG extends ModelQualityFunctionalTestNGBase {

    private String spFunctionalTestName = "deploymentTest_AT_SelectedPipeline";
    private String prodfunctionalTestName = "deploymentTest_AT_Production";
    private String functionalTestDatasetName = "deploymentTestDataSet_AT";

    @Test(groups = "manual")
    public void testmodelRunNameValidation() {
        String testStr = "Test12345";
        String pattern = "[^A-Za-z0-9_]";

        Assert.assertEquals(testStr.replaceAll(pattern, ""), "Test12345");

        testStr = "Test 12345";
        Assert.assertEquals(testStr.replaceAll(pattern, ""), "Test12345");

        testStr = "Test_12345";
        Assert.assertEquals(testStr.replaceAll(pattern, ""), "Test_12345");

        testStr = "T.e,s/t&1(2*3-4:5;?()[]{}\"\\'\t";
        Assert.assertEquals(testStr.replaceAll(pattern, ""), "Test12345");
    }

    @Test(groups = "functional", enabled = false)
    public void updateProductionAnalyticPipeline() {
        createDeploymentTestDataSet();
        // create a t-1 prod pipeline
        AnalyticPipelineServiceImpl spiedAnalyticPipelineService = spy(
                (AnalyticPipelineServiceImpl) analyticPipelineService);
        doReturn("z/9.9.8-SNAPSHOT").when(spiedAnalyticPipelineService).getLedsVersion();
        AnalyticPipeline oldProdAp = spiedAnalyticPipelineService.createLatestProductionAnalyticPipeline();

        // create a selected pipeline test and a prod test with the t-1 pipeline
        AnalyticTestEntityNames analyticTestEntityNames = new AnalyticTestEntityNames();
        analyticTestEntityNames.setName(spFunctionalTestName);
        analyticTestEntityNames.setAnalyticTestType(AnalyticTestType.SelectedPipelines);
        analyticTestEntityNames.setAnalyticPipelineNames(new ArrayList<>(Arrays.asList(oldProdAp.getName())));
        analyticTestEntityNames.setDataSetNames(new ArrayList<>(Arrays.asList(functionalTestDatasetName)));
        analyticTestService.createAnalyticTest(analyticTestEntityNames);

        analyticTestEntityNames = new AnalyticTestEntityNames();
        analyticTestEntityNames.setName(prodfunctionalTestName);
        analyticTestEntityNames.setAnalyticTestType(AnalyticTestType.Production);
        analyticTestEntityNames.setAnalyticPipelineNames(new ArrayList<>(Arrays.asList(oldProdAp.getName())));
        analyticTestEntityNames.setDataSetNames(new ArrayList<>(Arrays.asList(functionalTestDatasetName)));
        analyticTestService.createAnalyticTest(analyticTestEntityNames);

        // create a t prod pipeline
        doReturn("z/9.9.9-SNAPSHOT").when(spiedAnalyticPipelineService).getLedsVersion();
        AnalyticPipeline newProdAp = spiedAnalyticPipelineService.createLatestProductionAnalyticPipeline();

        // run update, verify tests and modelruns
        analyticTestService.updateProductionAnalyticPipeline();
        AnalyticTest spTest = analyticTestEntityMgr.findByName(spFunctionalTestName);
        AnalyticTest prodTest = analyticTestEntityMgr.findByName(prodfunctionalTestName);
        Assert.assertTrue(spTest.getAnalyticPipelines().get(0).getName().equals(newProdAp.getName()));
        Assert.assertTrue(prodTest.getAnalyticPipelines().get(0).getName().equals(newProdAp.getName()));

        List<ModelRun> spRuns = modelRunEntityMgr.findModelRunsByAnalyticTest(spTest);
        Assert.assertEquals(spRuns.size(), 1);
        List<ModelRun> prodRuns = modelRunEntityMgr.findModelRunsByAnalyticTest(prodTest);
        Assert.assertEquals(prodRuns.size(), 1);
    }

    @AfterTest(groups = "manual")
    public void cleanup() {
        AnalyticTest at = analyticTestEntityMgr.findByName(spFunctionalTestName);
        if (at != null) {
            analyticTestEntityMgr.delete(at);
        }
        at = analyticTestEntityMgr.findByName(prodfunctionalTestName);
        if (at != null) {
            analyticTestEntityMgr.delete(at);
        }

        AnalyticPipeline ap = analyticPipelineEntityMgr.findByName("PRODUCTION-z/9.9.9-SNAPSHOT");
        if (ap != null) {
            analyticPipelineEntityMgr.delete(ap);
        }

        ap = analyticPipelineEntityMgr.findByName("PRODUCTION-z/9.9.8-SNAPSHOT");
        if (ap != null) {
            analyticPipelineEntityMgr.delete(ap);
        }

        DataSet ds = dataSetEntityMgr.findByName(functionalTestDatasetName);
        if (ds != null) {
            dataSetEntityMgr.delete(ds);
        }
    }

    private DataSet createDeploymentTestDataSet() {
        DataSet dataSet = new DataSet();
        dataSet.setName(functionalTestDatasetName);
        dataSet.setIndustry("Industry1");
        dataSet.setTenant(new Tenant("Model_Quality_Test.Model_Quality_Test.Production"));
        dataSet.setDataSetType(DataSetType.FILE);
        dataSet.setSchemaInterpretation(SchemaInterpretation.SalesforceLead);
        dataSet.setTrainingSetHdfsPath(
                "/Pods/Default/Services/ModelQuality/Mulesoft_MKTO_LP3_ScoringLead_20160316_170113.csv");
        ScoringDataSet scoringDataSet = new ScoringDataSet();
        scoringDataSet.setName("ScoringDataSet1");
        scoringDataSet.setDataHdfsPath("ScoringDataSetPath1");
        dataSet.addScoringDataSet(scoringDataSet);

        dataSetEntityMgr.create(dataSet);
        return dataSet;
    }
}
