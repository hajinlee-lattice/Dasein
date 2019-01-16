package com.latticeengines.modelquality.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;

public class AnalyticPipelineServiceImplFunctionalTestNG extends ModelQualityFunctionalTestNGBase {

    @Test(groups = "functional", enabled = false)
    public void createLatestWithValidVersion() {
        try {
            AnalyticPipeline ap = analyticPipelineEntityMgr.getLatestProductionVersion();
            Integer initialVersion = 0;
            if (ap != null) {
                initialVersion = ap.getVersion();
            }

            AnalyticPipelineServiceImpl spiedAnalyticPipelineService = spy(
                    (AnalyticPipelineServiceImpl) analyticPipelineService);
            doReturn("z/9.9.8-SNAPSHOT").when(spiedAnalyticPipelineService).getLedsVersion();

            ap = spiedAnalyticPipelineService.createLatestProductionAnalyticPipeline();
            Assert.assertNotNull(ap);
            Assert.assertEquals(new Integer(initialVersion + 1), ap.getVersion());
            Assert.assertEquals("PRODUCTION-z_9.9.8-SNAPSHOT", ap.getName());
            Assert.assertNotNull(analyticPipelineEntityMgr.findByName("PRODUCTION-z_9.9.8-SNAPSHOT"));

            doReturn("z/9.9.9-SNAPSHOT").when(spiedAnalyticPipelineService).getLedsVersion();
            ap = spiedAnalyticPipelineService.createLatestProductionAnalyticPipeline();
            Assert.assertNotNull(ap);
            Assert.assertEquals(new Integer(initialVersion + 2), ap.getVersion());
            Assert.assertEquals("PRODUCTION-z_9.9.9-SNAPSHOT", ap.getName());
            Assert.assertNotNull(analyticPipelineEntityMgr.findByName("PRODUCTION-z_9.9.9-SNAPSHOT"));
        } catch (Exception e) {
            throw e;
        } finally {
            AnalyticPipeline ap = analyticPipelineEntityMgr.findByName("PRODUCTION-z_9.9.8-SNAPSHOT");
            if (ap != null) {
                analyticPipelineEntityMgr.delete(ap);
            }
            ap = analyticPipelineEntityMgr.findByName("PRODUCTION-z_9.9.9-SNAPSHOT");
            if (ap != null) {
                analyticPipelineEntityMgr.delete(ap);
            }
        }
    }

}
