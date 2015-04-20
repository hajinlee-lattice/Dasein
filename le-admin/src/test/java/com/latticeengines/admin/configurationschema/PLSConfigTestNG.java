package com.latticeengines.admin.configurationschema;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.TestLatticeComponent;

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
}
