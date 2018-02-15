package com.latticeengines.modeling.dataflow;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.serviceflows.modeling.dataflow.DedupEventTableParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

/**
 * Test for PLS-1286
 */
@ContextConfiguration(locations = { "classpath:serviceflows-modeling-dataflow-context.xml" })
public class DedupEventTableNoLocationColumnsTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional", enabled = false)
    public void test() {
        DedupEventTableParameters parameters = new DedupEventTableParameters("EventTable");
        executeDataFlow(parameters);
    }

    @Override
    protected String getFlowBeanName() {
        return "dedupEventTable";
    }

    @Override
    protected String getScenarioName() {
        return "noLocationColumns";
    }
}
