package com.latticeengines.modelquality.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;
import com.latticeengines.modelquality.service.SamplingService;

public class SamplingServiceImplFunctionalTestNG extends ModelQualityFunctionalTestNGBase {

    @Autowired
    private SamplingService samplingService;

    @Test(groups = "functional")
    public void createLatestWithValidVersion() {
        try {
            Sampling s = samplingEntityMgr.getLatestProductionVersion();
            Integer initialVersion = 0;
            if (s != null) {
                initialVersion = s.getVersion();
            }

            SamplingServiceImpl spiedSamplingService = spy((SamplingServiceImpl) samplingService);
            doReturn("z/9.9.8-SNAPSHOT").when(spiedSamplingService).getLedsVersion();

            s = spiedSamplingService.createLatestProductionSamplingConfig();
            Assert.assertNotNull(s);
            Assert.assertEquals(new Integer(initialVersion.intValue() + 1), s.getVersion());
            Assert.assertEquals("PRODUCTION-z_9.9.8-SNAPSHOT", s.getName());
            Assert.assertNotNull(samplingEntityMgr.findByName("PRODUCTION-z_9.9.8-SNAPSHOT"));

            doReturn("z/9.9.9-SNAPSHOT").when(spiedSamplingService).getLedsVersion();
            s = spiedSamplingService.createLatestProductionSamplingConfig();
            Assert.assertNotNull(s);
            Assert.assertEquals(new Integer(initialVersion + 2), s.getVersion());
            Assert.assertEquals("PRODUCTION-z_9.9.9-SNAPSHOT", s.getName());
            Assert.assertNotNull(samplingEntityMgr.findByName("PRODUCTION-z_9.9.9-SNAPSHOT"));
        } catch (Exception e) {
            throw e;
        } finally {
            Sampling s = samplingEntityMgr.findByName("PRODUCTION-z_9.9.8-SNAPSHOT");
            if (s != null) {
                samplingEntityMgr.delete(s);
            }
            s = samplingEntityMgr.findByName("PRODUCTION-z_9.9.9-SNAPSHOT");
            if (s != null) {
                samplingEntityMgr.delete(s);
            }
        }
    }

}
