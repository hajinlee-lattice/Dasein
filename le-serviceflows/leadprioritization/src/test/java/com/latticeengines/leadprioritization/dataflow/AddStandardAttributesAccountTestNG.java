package com.latticeengines.leadprioritization.dataflow;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-leadprioritization-dataflow-context.xml" })
public class AddStandardAttributesAccountTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        AddStandardAttributesParameters parameters = new AddStandardAttributesParameters("EventTable",
                TransformationPipeline.getTransforms(TransformationGroup.STANDARD),
                SchemaInterpretation.SalesforceAccount);
        executeDataFlow(parameters);
    }

    @Override
    protected String getFlowBeanName() {
        return "addStandardAttributes";
    }

    @Override
    protected String getScenarioName() {
        return "accountBased";
    }
}
