package com.latticeengines.admin.configurationschema;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class VisiDBDLConfigTestNG extends ConfigurationSchemaTestNGBase {

    @Override
    @BeforeMethod(groups = {"unit", "functional"})
    protected void setUp() throws Exception {
        super.setUp();
        this.component = new TestLatticeComponent();
        this.defaultJson = "vdbdl_default.json";
        this.metadataJson = "vdbdl_metadata.json";  // optional
        this.expectedJson = "vdbdl_expected.json";
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
    public void getDefaultConfiguration() throws IOException {
        DocumentDirectory dir = batonService.getDefaultConfiguration(this.component.getName());

        String tenant = dir.get("/DMDeployment").getDocument().getData();
        Assert.assertEquals(tenant, "DMDeployment");

        String visiDBName = dir.get("/VisiDB").getChild("VisiDBName").getDocument().getData();
        Assert.assertEquals(visiDBName, "test_tenant");
        String createNewVisiDB = dir.get("/VisiDB").getChild("CreateNewVisiDB").getDocument().getData();
        Assert.assertEquals(Boolean.parseBoolean(createNewVisiDB), true);

        String ownerEmail = dir.get("/DL").getChild("OwnerEmail").getDocument().getData();
        Assert.assertEquals(ownerEmail, "richard.liu@lattice-engines.com");
    }

}
