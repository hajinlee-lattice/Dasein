package com.latticeengines.admin.configurationschema;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.TestLatticeComponent;

public class CDLConfigTestNG extends ConfigurationSchemaTestNGBase {

    @Override
    @BeforeMethod(groups = { "unit", "functional" })
    protected void setUp() throws Exception {
        super.setUp();
        this.component = new TestLatticeComponent();
        this.defaultJson = "cdl_default.json";
        this.metadataJson = "cdl_metadata.json"; // optional
        this.expectedJson = "cdl_expected.json";
        setupPaths();
        uploadDirectory();
    }

    @Test(groups = "unit")
    public void testUnitMainFlow() {
        runUnitMainFlow();
    }

    @Test(groups = "functional")
    public void testDefaultConfigurationFunctional() {
        runFunctionalMainFlow();
    }

}
