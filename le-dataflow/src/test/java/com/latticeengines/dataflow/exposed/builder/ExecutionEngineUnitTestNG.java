package com.latticeengines.dataflow.exposed.builder;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.builder.engine.MapReduceExecutionEngine;
import com.latticeengines.dataflow.exposed.builder.engine.TezExecutionEngine;

public class ExecutionEngineUnitTestNG {

    @Test(groups = "unit")
    public void getMapReduceEngine() {
        ExecutionEngine engine = ExecutionEngine.get("MR");
        assertTrue(engine instanceof MapReduceExecutionEngine);
    }

    @Test(groups = "unit")
    public void getTezEngine() {
        ExecutionEngine engine = ExecutionEngine.get("TEZ");
        assertTrue(engine instanceof TezExecutionEngine);
    }

    @Test(groups = "unit")
    public void getUnknownEngine() {
        ExecutionEngine engine = ExecutionEngine.get("ABC");
        assertTrue(engine instanceof TezExecutionEngine);
    }

}
