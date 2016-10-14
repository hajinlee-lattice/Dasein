package com.latticeengines.modelquality.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.Assert;

import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.PropDataMatchType;
import com.latticeengines.modelquality.entitymgr.AnalyticTestEntityMgr;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class AnalyticTestEntityMgrImplTestNG extends ModelQualityFunctionalTestNGBase {

    @Autowired
    AnalyticTestEntityMgr analyticTestEntityMgr;

    private final String testAnalyticTest = "TestAnalyticTest";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        AnalyticTest at = analyticTestEntityMgr.findByName(testAnalyticTest);
        if (at != null)
            analyticTestEntityMgr.delete(at);
    }

    @Test(groups = "functional")
    public void createAnalyticTest() {
        AnalyticTest analyticTest = new AnalyticTest();
        analyticTest.setName(testAnalyticTest);
        analyticTest.setPropDataMatchType(PropDataMatchType.DNB);
        List<DataSet> dataSets = getTestDatasets().subList(0, 1);
        analyticTest.setDataSets(dataSets);
        List<AnalyticPipeline> analyticPipelines = getTestAnalyticPipelines().subList(0, 1);
        analyticTest.setAnalyticPipelines(analyticPipelines);
        analyticTestEntityMgr.create(analyticTest);

        AnalyticTest at = analyticTestEntityMgr.findByName(testAnalyticTest);
        Assert.assertNotNull(at);
        Assert.assertEquals(at.getPid(), analyticTest.getPid());
    }

    private List<AnalyticPipeline> getTestAnalyticPipelines() {
        // this should create a test analytic pipeline
        return analyticPipelineEntityMgr.findAll();
    }

    private List<DataSet> getTestDatasets() {
        // this should create a test dataset
        return dataSetEntityMgr.findAll();
    }
}
