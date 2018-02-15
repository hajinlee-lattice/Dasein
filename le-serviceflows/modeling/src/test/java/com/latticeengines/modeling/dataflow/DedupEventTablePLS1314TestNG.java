package com.latticeengines.modeling.dataflow;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.serviceflows.modeling.dataflow.DedupEventTableParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-modeling-dataflow-context.xml" })
public class DedupEventTablePLS1314TestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional", enabled = false)
    public void test() {
        DedupEventTableParameters parameters = new DedupEventTableParameters("EventTable");
        executeDataFlow(parameters);
    }

    @Override
    protected String getScenarioName() {
        return "pls1314";
    }

    @Override
    protected String getFlowBeanName() {
        return "dedupEventTable";
    }
}
