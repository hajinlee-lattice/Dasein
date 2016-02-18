package com.latticeengines.prospectdiscovery.dataflow;

import java.util.Arrays;
import java.util.List;

import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.CreateAttributeLevelSummaryParameters;

@ContextConfiguration(locations = { "classpath:serviceflows-prospectdiscovery-context.xml" })
public class CreateAttributeLevelSummaryTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {
    
    private CreateAttributeLevelSummaryParameters getStandardParameters() {
        List<String> groupByCols = Arrays.asList(new String[] { "BusinessIndustry", "AverageProbability" });
        CreateAttributeLevelSummaryParameters params = new CreateAttributeLevelSummaryParameters("ScoredEventTable", groupByCols, "Probability");
        params.aggregationType = "AVG";
        return params;
        
    }

    @Test(groups = "functional")
    public void execute() throws Exception {
        executeDataFlow(getStandardParameters());
    }

    @Override
    public String getFlowBeanName() {
        return "createAttributeLevelSummary";
    }

    @Override
    protected String getIdColumnName(String tableName) {
        return "LatticeAccountID";
    }

    @Override
    protected String getLastModifiedColumnName(String tableName) {
        return null;
    }
}
