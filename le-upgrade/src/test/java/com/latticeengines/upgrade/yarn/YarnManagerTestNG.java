package com.latticeengines.upgrade.yarn;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class YarnManagerTestNG extends UpgradeFunctionalTestNGBase {

    private final static String TEST_TUPLE_ID = CustomerSpace.parse(CUSTOMER).toString();
    private final static String TEST_MODEL_GUID = "ms__b99ddcc6-7ecb-45a0-b128-9664b51c1ce9-PLSModel";

    private String modelPath;

    @Autowired
    private YarnManager yarnManager;

    @Autowired
    private Configuration yarnConfiguration;

    @BeforeMethod(groups = "functional")
    public void beforeEach() throws Exception {
        yarnManager.deleteTupleIdCustomerRoot(CUSTOMER);
    }

//    @AfterMethod(groups = "functional")
//    public void afterEach() throws Exception {
//        yarnManager.deleteTupleIdCustomerRoot(CUSTOMER);
//    }

//    @Test(groups = "functional")
//    public void testDeleteTupleIdPath() throws Exception {
//        String customerRoot = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, CUSTOMER);
//        Assert.assertFalse(HdfsUtils.fileExists(yarnConfiguration, customerRoot));
//    }
//
//    @Test(groups = "functional", expectedExceptions = IllegalStateException.class)
//    public void testCopyCustomerNullSrc() throws Exception {
//        yarnManager.copyCustomerFromSingularToTupleId("nope");
//    }
//
//    @Test(groups = "functional", expectedExceptions = IllegalStateException.class)
//    public void testCopyCustomerExistingDest() throws Exception {
//        String customerRoot = YarnPathUtils.constructTupleIdCustomerRoot(customerBase, CUSTOMER);
//        HdfsUtils.mkdir(yarnConfiguration, customerRoot);
//        yarnManager.copyCustomerFromSingularToTupleId(CUSTOMER);
//    }
//
//    @Test(groups = "functional")
//    public void testCopyCustomer() throws Exception {
//        yarnManager.copyCustomerFromSingularToTupleId(CUSTOMER);
//
//        String modelsRoot = YarnPathUtils.constructTupleIdModelsRoot(customerBase, CUSTOMER);
//        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, modelsRoot),
//                String.format("models folder for customer %s has not been created.", CUSTOMER));
//
//        String dataRoot = YarnPathUtils.constructTupleIdDataRoot(customerBase, CUSTOMER);
//        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, dataRoot),
//                String.format("data folder for customer %s has not been created.", CUSTOMER));
//    }

    @Test(groups = "functional")
    public void testCopyModel() throws Exception {
        yarnManager.copyModelFromSingularToTupleId(CUSTOMER, MODEL_GUID);

        String modelPath = YarnPathUtils.constructTupleIdModelsRoot(customerBase, CUSTOMER)
                + "/" + EVENT_TABLE + "/" + UUID + "/" + CONTAINER_ID;
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, modelPath),
                String.format("model %s for customer %s cannot be found at %s.", MODEL_GUID, CUSTOMER, modelPath));
    }

//    @Test(groups = "functional")
//    public void testFindData() throws Exception {
//        String eventWithData = yarnManager.findAvaiableEventData(CUSTOMER);
//        Assert.assertEquals(eventWithData, EVENT_TABLE);
//    }

}


