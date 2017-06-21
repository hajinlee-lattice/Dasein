package com.latticeengines.prospectdiscovery.dataflow;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.dataflow.CreateScoreTableParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-prospectdiscovery-dataflow-context.xml" })
public class CreateScoreTableTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    private CreateScoreTableParameters getStandardParameters() {
        CreateScoreTableParameters params = new CreateScoreTableParameters("ScoreResult", "EventTable",
                "LatticeAccountID");
        return params;

    }

    @Test(groups = "functional")
    public void execute() throws Exception {
        executeDataFlow(getStandardParameters());
    }

    @Override
    public String getFlowBeanName() {
        return "createScoreTable";
    }

    @Override
    protected String getIdColumnName(String tableName) {
        if (tableName.equals("ScoreResult")) {
            return "LeadID";
        } else if (tableName.equals("EventTable")) {
            return "LatticeAccountID";
        }
        return "Id";
    }

    @Override
    protected String getLastModifiedColumnName(String tableName) {
        return null;
    }
}
