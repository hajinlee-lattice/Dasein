package com.latticeengines.modelquality.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.domain.exposed.modelquality.AnalyticTest;
import com.latticeengines.domain.exposed.modelquality.AnalyticTestEntityNames;
import com.latticeengines.domain.exposed.modelquality.DataSet;
import com.latticeengines.domain.exposed.modelquality.PropDataMatchType;
import com.latticeengines.modelquality.entitymgr.AnalyticTestEntityMgr;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;
import com.latticeengines.modelquality.service.AnalyticTestService;

public class AnalyticTestServiceImplFunctionalTestNG extends ModelQualityFunctionalTestNGBase {

    @Autowired
    private AnalyticTestService analyticTestService;
    
    @Autowired
    private AnalyticTestEntityMgr analyticTestEntityMgr;
    
    @BeforeClass(groups = "functional")
    public void setup()
    {
        AnalyticTest at = analyticTestEntityMgr.findByName("BadAnalyticTest");
        
        if(at != null)
        {
            analyticTestEntityMgr.delete(at);
        }
    }
    
    @Test(groups = "functional", expectedExceptions = RuntimeException.class)
    public void createAnalyticTestFailure() throws RuntimeException
    {
        AnalyticTestEntityNames analyticTestEntityNames = new AnalyticTestEntityNames();
        analyticTestEntityNames.setName("BadAnalyticTest");
        
        ArrayList<String> datasets = new ArrayList<String>(); 
        datasets.add("BadDatasetName");
        analyticTestEntityNames.setDataSetNames(datasets);
        
        ArrayList<String> analyticPipelines = new ArrayList<String>(); 
        analyticPipelines.add("BadPipelineName");
        analyticTestEntityNames.setAnalyticPipelineNames(analyticPipelines);
        
        analyticTestEntityNames.setPropDataMatchType(PropDataMatchType.DNB);
        
        AnalyticTest at = analyticTestService.createAnalyticTest(analyticTestEntityNames);
        
        at = analyticTestEntityMgr.findByName("BadAnalyticTest");
        Assert.assertNull(at);   
    }
    
    @Test(groups = "functional")
    public void createAnalyticTest()
    {
        AnalyticTestEntityNames analyticTestEntityNames = new AnalyticTestEntityNames();
        analyticTestEntityNames.setName("SrvImplTestAnalyticTest");
        
        analyticTestEntityNames.setDataSetNames(getTestDatasets());
        
        analyticTestEntityNames.setAnalyticPipelineNames(getTestAnalyticPipelines());
        
        analyticTestEntityNames.setPropDataMatchType(PropDataMatchType.DNB);
        
        AnalyticTest at = analyticTestService.createAnalyticTest(analyticTestEntityNames);
        
        at = analyticTestEntityMgr.findByName("SrvImplTestAnalyticTest");
        Assert.assertNotNull(at);   
        
        // Clean up
        analyticTestEntityMgr.delete(at);
        analyticTestEntityMgr.deleteAll();
    }
    
    private List<String> getTestAnalyticPipelines() {
        // this should create a test analytic pipeline
        List<String> aps = new ArrayList<String>();
        for (AnalyticPipeline ap : analyticPipelineEntityMgr.findAll())
        {
            aps.add(ap.getName());
        }
        return aps;   
    }

    private List<String> getTestDatasets() {
        // this should create a test dataset
        List<String> datasets = new ArrayList<String>();
        for (DataSet ds : dataSetEntityMgr.findAll())
        {
            datasets.add(ds.getName());
        }
        return datasets;
    }
    
}
