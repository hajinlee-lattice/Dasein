package com.latticeengines.modelquality.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.DataFlow;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;
import com.latticeengines.modelquality.service.DataFlowService;

public class DataFlowServiceImplFunctionalTestNG extends ModelQualityFunctionalTestNGBase {

    @Autowired
    private DataFlowService dataFlowService;

    @Test(groups = "functional")
    public void createLatestWithValidVersion() {
        try {
            DataFlowServiceImpl spiedDataFlowService = spy((DataFlowServiceImpl) dataFlowService);
            doReturn("z/9.9.9-SNAPSHOT").when(spiedDataFlowService).getVersion();

            DataFlow df = spiedDataFlowService.createLatestProductionDataFlow();
            Assert.assertNotNull(df);
            Assert.assertEquals("PRODUCTION-z_9.9.9-SNAPSHOT", df.getName());
            Assert.assertNotNull(dataFlowEntityMgr.findByName("PRODUCTION-z_9.9.9-SNAPSHOT"));
        } catch (Exception e) {
            throw e;
        } finally {
            DataFlow df = dataFlowEntityMgr.findByName("PRODUCTION-z_9.9.9-SNAPSHOT");
            if (df != null) {
                dataFlowEntityMgr.delete(df);
            }
        }
    }

}
