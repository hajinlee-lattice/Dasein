package com.latticeengines.pls.service.impl.fileprocessor.state;

import static org.testng.Assert.assertEquals;

import java.io.File;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BaseStateProcessorUnitTestNG {
    
    private BaseStateProcessor processor = new BaseStateProcessor();

    @Test(groups = "unit", dataProvider = "files")
    public void stripExtension(String fileName, String tenant, String strippedFileName) throws Exception {
        String[] tenantAndFile = processor.stripExtension(new File(fileName));
        assertEquals(tenantAndFile[0], tenant);
        assertEquals(tenantAndFile[1], strippedFileName);
    }
    
    @DataProvider(name = "files")
    public Object[][] getFiles() {
        return new String[][] { //
                { "DemoContract.DemoTenant.Production~somefilename.csv", "DemoContract.DemoTenant.Production", "somefilename" }, //
                { "somepayload.json", null, "somepayload" } //
        };
    }
}
