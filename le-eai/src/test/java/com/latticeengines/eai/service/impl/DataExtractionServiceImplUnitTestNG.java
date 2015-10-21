package com.latticeengines.eai.service.impl;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;

public class DataExtractionServiceImplUnitTestNG {
    
    private DataExtractionServiceImpl dataExtractionService = new DataExtractionServiceImpl();
    
    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterClass(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void createTargetPathForCustomerSpace() {
        String customerSpace = "X.Y.Production";
        String path = dataExtractionService.createTargetPath(customerSpace);
        assertEquals(path, "/Pods/Development/Contracts/X/Tenants/Y/Spaces/Production/Data/Tables");
    }

    @Test(groups = "unit")
    public void createTargetPathForTenant() {
        String customerSpace = "X";
        String path = dataExtractionService.createTargetPath(customerSpace);
        assertEquals(path, "/Pods/Development/Contracts/X/Tenants/X/Spaces/Production/Data/Tables");
    }
}
