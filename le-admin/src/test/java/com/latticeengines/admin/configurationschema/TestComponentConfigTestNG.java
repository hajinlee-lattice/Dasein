package com.latticeengines.admin.configurationschema;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class TestComponentConfigTestNG extends ConfigurationSchemaTestNGBase {

    @Override
    @BeforeMethod(groups = "unit")
    protected void setUp() throws Exception {
        super.setUp();
        this.componentName = "TestComponent";
        this.defaultJson = "testcomponent_default.json";
        this.metadataJson = "testcomponent_metadata.json";
        this.expectedJson = "testcomponent_expected.json";
        setupPaths();
    }

}
