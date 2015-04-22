package com.latticeengines.admin.configurationschema;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class PLSConfigTestNG extends ConfigurationSchemaTestNGBase {

    @Override
    @BeforeMethod(groups = {"unit", "functional"})
    protected void setUp() throws Exception {
        super.setUp();
        this.component = new TestLatticeComponent();
        this.defaultJson = "pls_default.json";
        this.metadataJson = "pls_metadata.json";  // optional
        this.expectedJson = "pls_expected.json";
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
    public void testConfig4() throws IOException {
        DocumentDirectory dir = batonService.getDefaultConfiguration(this.component.getName());

        String adminEmail = dir.get("/RootAdminEmail").getDocument().getData();
        Assert.assertEquals(adminEmail, "bnguyen@lattice-engines.com");
    }
}
