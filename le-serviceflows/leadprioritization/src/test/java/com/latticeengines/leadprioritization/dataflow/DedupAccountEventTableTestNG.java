package com.latticeengines.leadprioritization.dataflow;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.DedupEventTableParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-leadprioritization-context.xml" })
public class DedupAccountEventTableTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional", enabled = true)
    public void test() {
        DedupEventTableParameters parameters = new DedupEventTableParameters("EventTable", "PublicDomain");
        executeDataFlow(parameters);
    }

    @Override
    protected String getFlowBeanName() {
        return "dedupEventTable";
    }

    @Override
    protected String getScenarioName() {
        return "accountBased";
    }

    @Override
    protected String getLastModifiedColumnName(String tableName) {
        if (!tableName.equals("PublicDomain")) {
            return "LastModifiedDate";
        }
        return null;
    }
}
