package com.latticeengines.upgrade.yarn;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class YarnManagerTestNG extends UpgradeFunctionalTestNGBase {

    @Autowired
    private YarnManager yarnManager;

    @Autowired
    private Configuration yarnConfiguration;

    @BeforeMethod(groups = "functional")
    public void beforeEach() throws Exception {
        yarnManager.deleteTupleIdCustomerRoot(CUSTOMER);
    }

    @AfterMethod(groups = "functional")
    public void afterEach() throws Exception {
        yarnManager.deleteTupleIdCustomerRoot(CUSTOMER);
    }

    @Test(groups = "functional")
    public void testDeleteTupleIdPath() throws Exception {
        String customerRoot = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, CUSTOMER);
        Assert.assertFalse(HdfsUtils.fileExists(yarnConfiguration, customerRoot));
    }

    @Test(groups = "functional")
    public void testCopyModel() throws Exception {
        yarnManager.copyModelsFromSingularToTupleId(CUSTOMER);
        yarnManager.fixModelName(CUSTOMER, MODEL_GUID);
        String modelPath = YarnPathUtils.constructTupleIdModelsRoot(customerBase, CUSTOMER)
                + "/" + EVENT_TABLE + "/" + UUID + "/" + CONTAINER_ID;
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, modelPath),
                String.format("model %s for customer %s cannot be found at %s.", MODEL_GUID, CUSTOMER, modelPath));
    }

    @Test(groups = "functional")
    public void testCopyData() throws Exception {
        yarnManager.copyModelsFromSingularToTupleId(CUSTOMER);

        String dataPath = YarnPathUtils.constructTupleIdModelsRoot(customerBase, CUSTOMER)
                + "/" + EVENT_TABLE;
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, dataPath),
                String.format("data for customer %s cannot be found at %s.", CUSTOMER, dataPath));
    }

//    @Test(groups = "functional")
//    public void testGenerateModelSummary() {
//        yarnManager.generateModelSummary(CUSTOMER, MODEL_GUID);
//        Assert.fail();
//    }

}


