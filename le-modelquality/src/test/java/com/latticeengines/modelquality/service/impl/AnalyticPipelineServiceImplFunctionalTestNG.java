package com.latticeengines.modelquality.service.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;
import com.latticeengines.modelquality.service.AnalyticPipelineService;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

import org.testng.Assert;

public class AnalyticPipelineServiceImplFunctionalTestNG extends ModelQualityFunctionalTestNGBase {

    @Autowired
    private AnalyticPipelineService analyticPipelineService;

    private InternalResourceRestApiProxy proxy = null;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        super.cleanupDb();
        super.cleanupHdfs();

        proxy = mock(InternalResourceRestApiProxy.class);
        Map<String, String> activeStack = new HashMap<>();
        activeStack.put("CurrentStack", "");
        when(proxy.getActiveStack()).thenReturn(activeStack);
        ReflectionTestUtils.setField(analyticPipelineService, "internalResourceRestApiProxy", proxy);
    }

    @Test(groups = "functional")
    public void createLatestProductionPipeline() {
        AnalyticPipeline ap = analyticPipelineService.createLatestProductionAnalyticPipeline();
        Assert.assertNotNull(ap);
    }
}
