package com.latticeengines.upgrade.jdbc;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class BardJdbcManagerTestNG extends UpgradeFunctionalTestNGBase{

    @Autowired
    private BardJdbcManager bardJdbcManager;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        bardJdbcManager.init("Lattice_Relaunch_DB_BARD", "");
    }
    
    @Test(groups = "functional")
    public void testActiveModelKey() throws Exception {
        List<String> activeModelKey = bardJdbcManager.getActiveModelKey();
        Assert.assertTrue(activeModelKey.size() == 1, "Didn't find exactly 1 active model.");
        Assert.assertFalse(activeModelKey.get(0).contains(","), "Found more than 1 active model.");
    }
    
}
