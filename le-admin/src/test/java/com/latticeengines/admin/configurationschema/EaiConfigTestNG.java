package com.latticeengines.admin.configurationschema;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class EaiConfigTestNG extends ConfigurationSchemaTestNGBase{

    @Override
    @BeforeMethod(groups = {"unit", "functional"})
    protected void setUp() throws Exception {
        super.setUp();
        this.component = new TestLatticeComponent();
        this.defaultJson = "eai_default.json";
        this.metadataJson = "eai_metadata.json";  // optional
        this.expectedJson = "eai_expected.json";
        setupPaths();
        uploadDirectory();
    }

    @Test(groups = "unit")
    public void testUnitMainFlow() { runUnitMainFlow(); }

    @Test(groups = "functional")
    public void testDefaultConfigurationFuncational() { runFunctionalMainFlow(); }

    /*
    ================================================================================
        Test how you want to use the configuration
    ================================================================================
    */

    /**
     * this test demonstrate how to get configuration using DocumentDirectory
     */
    @Test(groups = "unit")
    public void testConfig() throws IOException {
        DocumentDirectory dir = batonService.getDefaultConfiguration(this.component.getName());

        String connectTimeout = dir.get("/SalesforceEndpointConfig").getChild("HttpClient").getChild("ConnectTimeout").getDocument().getData();
        Assert.assertEquals(connectTimeout, "60000");

        String importTimeout = dir.get("/SalesforceEndpointConfig").getChild("HttpClient").getChild("ImportTimeout").getDocument().getData();
        Assert.assertEquals(importTimeout, "3600000");
    }
}
