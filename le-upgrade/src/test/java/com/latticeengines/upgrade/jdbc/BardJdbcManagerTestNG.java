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

    @Test(groups = "functional")
    public void testGetModelGuidsWithinLast2Weeks() throws Exception {
        bardJdbcManager.init("CitrixSaas_PLS2_DB_BARD", "");
        List<String> modelKeys = bardJdbcManager.getModelGuidsWithinLast2Weeks();
        Assert.assertTrue(modelKeys.size() == 6, "Didn't find exactly 6 models within last 2 weeks.");
    }

    @Test(groups = "functional")
    public void testGetMOdelContents(){
        bardJdbcManager.init("CitrixSaas_PLS2_DB_BARD", "");
        String modelContent = bardJdbcManager.getModelContent("Model_ms__245bf5af-c420-4657-996e-647e8a07ab63-PLSModel");
        Assert.assertTrue(modelContent.startsWith("dt"));
    }
}
