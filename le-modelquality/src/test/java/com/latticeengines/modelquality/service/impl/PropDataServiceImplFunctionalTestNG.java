package com.latticeengines.modelquality.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.List;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
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
            PropDataServiceImpl spiedPropDataService = spy((PropDataServiceImpl) propDataService);
            doReturn("z/9.9.9-SNAPSHOT").when(spiedPropDataService).getVersion();

            PropData propdata = spiedPropDataService.createLatestProductionPropData();
            Assert.assertNotNull(propdata);
            Assert.assertEquals("PRODUCTION-z_9.9.9-SNAPSHOT", propdata.getName());
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

    @Test(groups = "functional")
    public void createLatestforUIForDefaultDNB() {     
        List<PropData> propdatas = propDataService.createLatestProductionPropDatasForUI();
        
        Assert.assertEquals(propdatas.size(), 5);
        for(PropData pd : propdatas)
        {
            if(pd.getName().contains("DNB"))
            {
                Assert.assertTrue(pd.getName().contains("2.0.0"));
            }
        }
    }
    
    @Test(groups = "functional")
    public void createLatestforUIForCustomDNB() {
        PropDataServiceImpl spiedPropDataService = spy(((PropDataServiceImpl)propDataService));
        doReturn("2.0.1").when(spiedPropDataService).getLatestDNBVersion();
        
        List<PropData> propdatas = spiedPropDataService.createLatestProductionPropDatasForUI();
        
        Assert.assertEquals(propdatas.size(), 5);
        for(PropData pd : propdatas)
        {
            if(pd.getName().contains("DNB"))
            {
                Assert.assertFalse(pd.getName().contains("2.0.0"));
                Assert.assertTrue(pd.getName().contains("2.0.1"));
            }
        }        
    }
    
    

}
