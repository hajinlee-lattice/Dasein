package com.latticeengines.domain.exposed.dataflow;

import static org.testng.Assert.assertTrue;

import java.util.HashMap;

import org.testng.annotations.Test;

public class DataFlowContextUnitTestNG {
    
    private DataFlowContext ctx = new DataFlowContext();

    @Test(groups = "unit")
    public void setProperty() {
        ctx.setProperty("xyz", new HashMap<>());
    }

    @Test(groups = "unit", dependsOnMethods = { "setProperty" })
    public void containsProperty() {
        assertTrue(ctx.containsProperty("xyz"));
    }

    @Test(groups = "unit", dependsOnMethods = { "setProperty" })
    public void getProperty() {
        assertTrue(ctx.getProperty("xyz", HashMap.class) instanceof HashMap);
    }

}
