package com.latticeengines.domain.exposed.pls;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ImportActionConfigurationUnitTestNG {

    @Test(groups = "unit")
    public void testNullConfig() {
        ImportActionConfiguration config = new ImportActionConfiguration();
        Assert.assertTrue(config.getImportCount()== 0);
    }
}
