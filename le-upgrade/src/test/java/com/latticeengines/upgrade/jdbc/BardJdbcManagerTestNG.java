package com.latticeengines.upgrade.jdbc;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class BardJdbcManagerTestNG extends UpgradeFunctionalTestNGBase{

    @Autowired
    private BardJdbcManager bardJdbcManager;

    @Test(groups = "functional")
    public void testOneActiveModelKey() throws Exception {
        bardJdbcManager.init("Lattice_Relaunch_DB_BARD", "");
        List<String> activeModelKey = bardJdbcManager.getActiveModelKey();
        Assert.assertTrue(activeModelKey.size() == 1, "Didn't find exactly 1 active model.");
    }

    @Test(groups = "functional")
    public void testFourActiveModelKeys() throws Exception {
        bardJdbcManager.init("PayPalPLS_DB_BARD", "");
        List<String> activeModelKey = bardJdbcManager.getActiveModelKey();
        Assert.assertTrue(activeModelKey.size() == 4, "Didn't find exactly 4 active models.");
    }
}
