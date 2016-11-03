package com.latticeengines.modelquality.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.AnalyticTestEntityNames;
import com.latticeengines.domain.exposed.modelquality.AnalyticTestType;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.ModelRun;
import com.latticeengines.domain.exposed.modelquality.PropDataMatchType;
import com.latticeengines.modelquality.entitymgr.AnalyticTestEntityMgr;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;
import com.latticeengines.modelquality.service.AnalyticTestService;

public class AnalyticTestServiceImplFunctionalTestNG extends ModelQualityFunctionalTestNGBase {

    @Autowired
    private AnalyticTestService analyticTestService;

    @Autowired
    private AnalyticTestEntityMgr analyticTestEntityMgr;

    private String createdModelRunName = null;

    @Test(groups = "functional", expectedExceptions = RuntimeException.class)
    public void createAnalyticTestFailure() throws RuntimeException {
        AnalyticTestEntityNames analyticTestEntityNames = new AnalyticTestEntityNames();
        analyticTestEntityNames.setName("BadAnalyticTest");

        ArrayList<String> datasets = new ArrayList<String>();
        datasets.add("BadDatasetName");
        analyticTestEntityNames.setDataSetNames(datasets);

        ArrayList<String> analyticPipelines = new ArrayList<String>();
        analyticPipelines.add("BadPipelineName");
        analyticTestEntityNames.setAnalyticPipelineNames(analyticPipelines);

        analyticTestEntityNames.setPropDataMatchType(PropDataMatchType.DNB);
        analyticTestEntityNames.setAnalyticTestTag("Test");

        AnalyticTest at = analyticTestService.createAnalyticTest(analyticTestEntityNames);

        at = analyticTestEntityMgr.findByName("BadAnalyticTest");
        Assert.assertNull(at);
    }

    @Test(groups = "functional")
    public void createAnalyticTest() {
        AnalyticTestEntityNames analyticTestEntityNames = new AnalyticTestEntityNames();
        analyticTestEntityNames.setName("SrvImplTestAnalyticTest");
        analyticTestEntityNames.setDataSetNames(getTestDatasets());
        analyticTestEntityNames.setAnalyticPipelineNames(getTestAnalyticPipelines());
        analyticTestEntityNames.setPropDataMatchType(PropDataMatchType.DNB);
        analyticTestEntityNames.setAnalyticTestTag("Test");
        analyticTestEntityNames.setAnalyticTestType(AnalyticTestType.Default);
        AnalyticTest at = analyticTestService.createAnalyticTest(analyticTestEntityNames);

        at = analyticTestEntityMgr.findByName("SrvImplTestAnalyticTest");
        Assert.assertNotNull(at);
        Assert.assertEquals(at.getAnalyticTestTag(), "Test");
        Assert.assertEquals(at.getAnalyticTestType(), AnalyticTestType.Default);
    }

    @Test(groups = "functional", dependsOnMethods = { "createAnalyticTest" })
    public void getAnalyticTest() {
        AnalyticTestEntityNames atn = analyticTestService.getByName("SrvImplTestAnalyticTest");
        Assert.assertNotNull(atn);
    }

    @Test(groups = "functional", dependsOnMethods = { "getAnalyticTest" })
    public void executeAnalyticTest() {
        List<ModelRun> results = analyticTestService.executeByName("SrvImplTestAnalyticTest");
        Assert.assertNotNull(results);
        Assert.assertEquals(results.size(), 1);

        AnalyticTest at = analyticTestEntityMgr.findByName("SrvImplTestAnalyticTest");
        Assert.assertEquals(at.isExecuted(), true);

        List<ModelRun> results2 = analyticTestService.executeByName("SrvImplTestAnalyticTest");
        Assert.assertEquals(results.size(), results2.size());
        Assert.assertEquals(results.get(0).getModelId(), results2.get(0).getModelId());
        createdModelRunName = results.get(0).getName();
    }

    @AfterTest(groups = "functional")
    public void cleanUp() {
        AnalyticTest at = analyticTestEntityMgr.findByName("BadAnalyticTest");

        if (at != null) {
            analyticTestEntityMgr.delete(at);
        }
        if (createdModelRunName != null || !createdModelRunName.isEmpty()) {
            modelRunEntityMgr.delete(modelRunEntityMgr.findByName(createdModelRunName));
        }
        at = analyticTestEntityMgr.findByName("SrvImplTestAnalyticTest");
        if (at != null) {
            analyticTestEntityMgr.delete(at);
        }
    }

    @BeforeClass(groups = "functional")
    public void setUp() {
        AnalyticTest at = analyticTestEntityMgr.findByName("BadAnalyticTest");
        if (at != null) {
            analyticTestEntityMgr.delete(at);
        }
        at = analyticTestEntityMgr.findByName("SrvImplTestAnalyticTest");
        if (at != null) {
            analyticTestEntityMgr.delete(at);
        }
    }

    private List<String> getTestAnalyticPipelines() {
        // this should create a test analytic pipeline
        List<String> aps = new ArrayList<String>();
        for (AnalyticPipeline ap : analyticPipelineEntityMgr.findAll()) {
            aps.add(ap.getName());
        }
        return aps;
    }

    private List<String> getTestDatasets() {
        // this should create a test dataset
        List<String> datasets = new ArrayList<String>();
        for (DataSet ds : dataSetEntityMgr.findAll()) {
            datasets.add(ds.getName());
        }
        return datasets;
    }

}
