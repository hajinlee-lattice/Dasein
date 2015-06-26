package com.latticeengines.upgrade.dl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class DataLoaderManagerTestNG extends UpgradeFunctionalTestNGBase {

    @Autowired
    private DataLoaderManager dataLoaderManager;

    @Test(groups = "functional")
    public void testConstructSpaceConfiguration() {
        SpaceConfiguration spaceConfiguration = dataLoaderManager.constructSpaceConfiguration(CUSTOMER);
        Assert.assertEquals(spaceConfiguration.getDlAddress(), DL_URL);
        Assert.assertEquals(spaceConfiguration.getTopology(), CRMTopology.MARKETO);
    }

}


