package com.latticeengines.leadprioritization.dataflow;

import static org.junit.Assert.assertNotNull;
import static org.testng.Assert.assertNotEquals;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = {"classpath:serviceflows-leadprioritization-context.xml"})
public class AddStandardAttributesLeadTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        AddStandardAttributesParameters parameters = new AddStandardAttributesParameters("EventTable");
        Table table = executeDataFlow(parameters);
        Attribute attribute = table.getAttribute("Title_Level");
        String displayName = attribute.getDisplayName();
        assertNotEquals(displayName, attribute.getName());
        assertNotNull(attribute.getName());
        attribute = table.getAttribute("Domain_Length");
        displayName = attribute.getDisplayName();
        assertNotEquals(displayName, attribute.getName());
        assertNotNull(attribute.getName());
    }

    @Override
    protected String getFlowBeanName() {
        return "addStandardAttributes";
    }

    @Override
    protected String getScenarioName() {
        return "leadBased";
    }
}
