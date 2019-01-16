package com.latticeengines.modelquality.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modelquality.PropData;
import com.latticeengines.modelquality.functionalframework.ModelQualityFunctionalTestNGBase;
import com.latticeengines.modelquality.service.PropDataService;

public class PropDataServiceImplFunctionalTestNG extends ModelQualityFunctionalTestNGBase {

    @Autowired
    private PropDataService propDataService;

    @Test(groups = "functional")
    public void createLatestWithValidVersion() {
        try {
            PropData pd = propDataEntityMgr.getLatestProductionVersion();
            Integer initialVersion = 0;
            if (pd != null) {
                initialVersion = pd.getVersion();
            }

            PropDataServiceImpl spiedPropDataService = spy((PropDataServiceImpl) propDataService);
            doReturn("z/9.9.8-SNAPSHOT").when(spiedPropDataService).getLedsVersion();

            pd = spiedPropDataService.createLatestProductionPropData();
            Assert.assertNotNull(pd);
            Assert.assertEquals(new Integer(initialVersion + 1), pd.getVersion());
            Assert.assertEquals("PRODUCTION-z_9.9.8-SNAPSHOT", pd.getName());
            Assert.assertNotNull(propDataEntityMgr.findByName("PRODUCTION-z_9.9.8-SNAPSHOT"));

            doReturn("z/9.9.9-SNAPSHOT").when(spiedPropDataService).getLedsVersion();
            pd = spiedPropDataService.createLatestProductionPropData();
            Assert.assertNotNull(pd);
            Assert.assertEquals(new Integer(initialVersion + 2), pd.getVersion());
            Assert.assertEquals("PRODUCTION-z_9.9.9-SNAPSHOT", pd.getName());
            Assert.assertNotNull(propDataEntityMgr.findByName("PRODUCTION-z_9.9.9-SNAPSHOT"));
        } catch (Exception e) {
            throw e;
        } finally {
            PropData pd = propDataEntityMgr.findByName("PRODUCTION-z_9.9.8-SNAPSHOT");
            if (pd != null) {
                propDataEntityMgr.delete(pd);
            }
            pd = propDataEntityMgr.findByName("PRODUCTION-z_9.9.9-SNAPSHOT");
            if (pd != null) {
                propDataEntityMgr.delete(pd);
            }
        }
    }

    @Test(groups = "functional")
    public void createLatestforUIForCustomDNB() {
        PropDataServiceImpl spiedPropDataService = spy(((PropDataServiceImpl) propDataService));
        doReturn("2.0.1").when(spiedPropDataService).getLatestDNBVersion();

        List<PropData> propdatas = spiedPropDataService.createLatestProductionPropDatasForUI();

        Assert.assertEquals(propdatas.size(), 5);
        for (PropData pd : propdatas) {
            if (pd.getName().contains("DNB")) {
                Assert.assertFalse(pd.getName().contains("2.0.0"));
                Assert.assertTrue(pd.getName().contains("2.0.1"));
            }
        }
    }

}
