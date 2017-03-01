package com.latticeengines.cdl.dataflow.resolve;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.cdl.FieldLoadStrategy;
import com.latticeengines.domain.exposed.dataflow.flows.cdl.ResolveStagingAndRuntimeTableParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-context.xml" })
public class ResolveWithFieldLoadRuleUpdateTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    /**
     * Id 19853828 will have null CustomerAtRiskDescription and PhoneNumber
     * Resulting record should maintain existing values of 
     */
    @Test(groups = "functional")
    public void test() {
        ResolveStagingAndRuntimeTableParameters parameters = new ResolveStagingAndRuntimeTableParameters();
        parameters.fieldLoadStrategy = FieldLoadStrategy.Update;
        parameters.stageTableName = "AccountStaging";
        parameters.runtimeTableName = "Account";
        executeDataFlow(parameters);
    }

    @Override
    protected String getFlowBeanName() {
        return "resolveStagingAndRuntimeTable";
    }
    
    @Override
    protected String getScenarioName() {
        return "nonEmptyRuntimeTable";
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
