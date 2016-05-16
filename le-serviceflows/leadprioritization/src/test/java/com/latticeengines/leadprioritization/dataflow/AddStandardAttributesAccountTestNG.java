package com.latticeengines.leadprioritization.dataflow;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-leadprioritization-context.xml" })
public class AddStandardAttributesAccountTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        AddStandardAttributesParameters parameters = new AddStandardAttributesParameters("EventTable", TransformationGroup.STANDARD);
        executeDataFlow(parameters);
    }

    @Override
    protected String getFlowBeanName() {
        return "addStandardAttributesViaJavaFunction";
    }

    @Override
    protected String getScenarioName() {
        return "accountBased";
    }
}
