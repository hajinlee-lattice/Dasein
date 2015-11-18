package com.latticeengines.admin.configurationschema;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.TestLatticeComponent;

public class MetadataConfigTestNG extends ConfigurationSchemaTestNGBase {

    @Override
    @BeforeMethod(groups = { "unit", "functional" })
    protected void setUp() throws Exception {
        super.setUp();
        this.component = new TestLatticeComponent();
        this.defaultJson = "metadata_default.json";
        this.metadataJson = "metadata_metadata.json"; // optional
        this.expectedJson = "metadata_expected.json";
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
}
