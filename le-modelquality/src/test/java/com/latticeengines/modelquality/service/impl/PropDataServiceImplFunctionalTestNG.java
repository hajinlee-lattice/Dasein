package com.latticeengines.modelquality.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;
import com.latticeengines.modelquality.service.PropDataService;

public class PropDataServiceImplFunctionalTestNG extends ModelQualityFunctionalTestNGBase {

    @Autowired
    private PropDataService PropDataService;

    @Test(groups = "functional")
    public void createLatestWithValidVersion() {
        try {
            PropDataServiceImpl spiedPropDataService = spy((PropDataServiceImpl) PropDataService);
            doReturn("z/9.9.9-SNAPSHOT").when(spiedPropDataService).getVersion();

            PropData df = spiedPropDataService.createLatestProductionPropData();
            Assert.assertNotNull(df);
            Assert.assertEquals("PRODUCTION-z_9.9.9-SNAPSHOT", df.getName());
            Assert.assertNotNull(propDataEntityMgr.findByName("PRODUCTION-z_9.9.9-SNAPSHOT"));
        } catch (Exception e) {
            throw e;
        } finally {
            PropData df = propDataEntityMgr.findByName("PRODUCTION-z_9.9.9-SNAPSHOT");
            if (df != null) {
                propDataEntityMgr.delete(df);
            }
        }
    }

}
