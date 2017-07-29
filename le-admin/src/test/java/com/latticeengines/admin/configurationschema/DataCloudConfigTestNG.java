package com.latticeengines.admin.configurationschema;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.TestLatticeComponent;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class DataCloudConfigTestNG extends ConfigurationSchemaTestNGBase {

    @Override
    @BeforeMethod(groups = {"unit", "functional"})
    protected void setUp() throws Exception {
        super.setUp();
        this.component = new TestLatticeComponent();
        this.defaultJson = "datacloud_default.json";
        this.metadataJson = "datacloud_metadata.json";
        this.expectedJson = "datacloud_expected.json";
        setupPaths();
        uploadDirectory();
    }

    @Test(groups = "unit")
    public void testUnitMainFlow() {
        runUnitMainFlow();
    }

    @Test(groups = "functional")
    public void testDefaultConfigurationFuncational() {
        runFunctionalMainFlow();
    }

    /*
     * ==========================================================================
     * ====== Test how you want to use the configuration
     * ========================
     * ========================================================
     */

    /**
     * this test demonstrate how to get configuration using DocumentDirectory
     */
    @Test(groups = "unit")
    public void testConfig() throws IOException {
        DocumentDirectory dir = batonService.getDefaultConfiguration(this.component.getName());

        String valueInZK = dir.get("/BypassDnBCache").getDocument().getData();

        assertFalse(Boolean.valueOf(valueInZK));

        DocumentDirectory.Node node = dir.get("/BypassDnBCache");
        node.getDocument().setData(String.valueOf(true));

        valueInZK = dir.get("/BypassDnBCache").getDocument().getData();

        assertTrue(Boolean.valueOf(valueInZK));
    }
}
