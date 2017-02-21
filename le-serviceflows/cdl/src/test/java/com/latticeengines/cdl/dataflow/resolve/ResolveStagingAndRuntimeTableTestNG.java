package com.latticeengines.cdl.dataflow.resolve;

import org.springframework.test.context.ContextConfiguration;

import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-context.xml" })
public class ResolveStagingAndRuntimeTableTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Override
    protected String getFlowBeanName() {
        return "resolveStagingAndRuntimeTable";
    }
    
    @Override
    protected String getScenarioName() {
        return "emptyRuntimeTable";
    }

}
