package com.latticeengines.modelquality.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;
import com.latticeengines.modelquality.service.DataFlowService;
public class DataFlowServiceImplFunctionalTestNG extends ModelQualityFunctionalTestNGBase {

    @Inject
    private DataFlowService dataFlowService;

    @Test(groups = "functional")
    public void createLatestWithValidVersion() {
        try {
            DataFlow df = dataFlowEntityMgr.getLatestProductionVersion();
            Integer initialVersion = 0;
            if (df != null) {
                initialVersion = df.getVersion();
            }

            DataFlowServiceImpl spiedDataFlowService = spy((DataFlowServiceImpl) dataFlowService);
            doReturn("z/9.9.8-SNAPSHOT").when(spiedDataFlowService).getLedsVersion();

            df = spiedDataFlowService.createLatestProductionDataFlow();
            Assert.assertNotNull(df);
            Assert.assertEquals(Integer.valueOf(initialVersion + 1), df.getVersion());
            Assert.assertEquals("PRODUCTION-z_9.9.8-SNAPSHOT", df.getName());
            Assert.assertNotNull(dataFlowEntityMgr.findByName("PRODUCTION-z_9.9.8-SNAPSHOT"));

            doReturn("z/9.9.9-SNAPSHOT").when(spiedDataFlowService).getLedsVersion();
            df = spiedDataFlowService.createLatestProductionDataFlow();
            Assert.assertNotNull(df);
            Assert.assertEquals(Integer.valueOf(initialVersion + 2), df.getVersion());
            Assert.assertEquals("PRODUCTION-z_9.9.9-SNAPSHOT", df.getName());
            Assert.assertNotNull(dataFlowEntityMgr.findByName("PRODUCTION-z_9.9.9-SNAPSHOT"));
        } catch (Exception e) {
            throw e;
        } finally {
            DataFlow df = dataFlowEntityMgr.findByName("PRODUCTION-z_9.9.8-SNAPSHOT");
            if (df != null) {
                dataFlowEntityMgr.delete(df);
            }
            df = dataFlowEntityMgr.findByName("PRODUCTION-z_9.9.9-SNAPSHOT");
            if (df != null) {
                dataFlowEntityMgr.delete(df);
            }
        }
    }

}
