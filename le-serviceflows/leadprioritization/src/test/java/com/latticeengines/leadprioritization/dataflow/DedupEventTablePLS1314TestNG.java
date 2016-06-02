package com.latticeengines.leadprioritization.dataflow;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.DedupEventTableParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-leadprioritization-context.xml" })
public class DedupEventTablePLS1314TestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        DedupEventTableParameters parameters = new DedupEventTableParameters("EventTable", "PublicDomain");
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
