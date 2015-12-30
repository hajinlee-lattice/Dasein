package com.latticeengines.prospectdiscovery.dataflow;

import java.util.Arrays;
import java.util.List;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.CreateAttributeLevelSummaryParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-prospectdiscovery-context.xml" })
public class CreateAttributeLevelSummaryTestNG extends ServiceFlowsFunctionalTestNGBase {
    
    private CreateAttributeLevelSummaryParameters getStandardParameters() {
        List<String> groupByCols = Arrays.asList(new String[] { "BusinessIndustry", "Event_IsWon" });
        CreateAttributeLevelSummaryParameters params = new CreateAttributeLevelSummaryParameters("ScoredEventTable", groupByCols, "Id");
        return params;
        
    }

    @Test(groups = "functional")
    public void execute() throws Exception {
        Table result = executeDataFlow(getStandardParameters());
    }

    @Override
    public String getFlowBeanName() {
        return "createAttributeLevelSummary";
    }

    @Override
    protected String getIdColumnName(String tableName) {
        return "Id";
    }

    @Override
    protected String getLastModifiedColumnName(String tableName) {
        return null;
    }
}
