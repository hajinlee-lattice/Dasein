package com.latticeengines.modelquality.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.Sampling;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;
import com.latticeengines.modelquality.service.SamplingService;

public class SamplingServiceImplFunctionalTestNG extends ModelQualityFunctionalTestNGBase {

    @Autowired
    private SamplingService SamplingService;

    @Test(groups = "functional")
    public void createLatestWithValidVersion() {
        try {
            SamplingServiceImpl spiedSamplingService = spy((SamplingServiceImpl) SamplingService);
            doReturn("z/9.9.9-SNAPSHOT").when(spiedSamplingService).getVersion();

            Sampling df = spiedSamplingService.createLatestProductionSamplingConfig();
            Assert.assertNotNull(df);
            Assert.assertEquals("PRODUCTION-z_9.9.9-SNAPSHOT", df.getName());
            Assert.assertNotNull(samplingEntityMgr.findByName("PRODUCTION-z_9.9.9-SNAPSHOT"));
        } catch (Exception e) {
            throw e;
        } finally {
            Sampling df = samplingEntityMgr.findByName("PRODUCTION-z_9.9.9-SNAPSHOT");
            if (df != null) {
                samplingEntityMgr.delete(df);
            }
        }
    }

}
