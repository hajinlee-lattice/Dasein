package com.latticeengines.admin.configurationschema;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.TestLatticeComponent;

public class ComponentConfigMinimumTestNG extends ConfigurationSchemaTestNGBase {

    @Override
    @BeforeMethod(groups = "functional")
    protected void setUp() throws Exception {
        super.setUp();
        this.component = new TestLatticeComponent();
        this.expectedJson = "testcomponent_expected.json";
    }

    @Test(groups = "functional")
    public void testDefaultConfigurationFuncational() { runFunctionalMainFlow(); }

}
