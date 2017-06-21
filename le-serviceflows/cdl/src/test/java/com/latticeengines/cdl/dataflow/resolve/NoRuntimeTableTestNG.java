package com.latticeengines.cdl.dataflow.resolve;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.ResolveStagingAndRuntimeTableParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-dataflow-context.xml" })
public class NoRuntimeTableTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        ResolveStagingAndRuntimeTableParameters parameters = new ResolveStagingAndRuntimeTableParameters();
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
        return "noRuntimeTable";
    }

    @Override
    protected String getIdColumnName(String tableName) {
        return "RowId";
    }

    @Override
    protected String getLastModifiedColumnName(String tableName) {
        return null;
    }

}
