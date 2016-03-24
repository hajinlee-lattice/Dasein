package com.latticeengines.leadprioritization.dataflow;

import static org.junit.Assert.assertNotNull;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dataflow.flows.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = {"classpath:serviceflows-leadprioritization-context.xml"})
public class AddStandardAttributesLeadTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        AddStandardAttributesParameters parameters = new AddStandardAttributesParameters("EventTable");
        Table table = executeDataFlow(parameters);
        Attribute attribute = table.getAttribute("Title_Level");
        assertNotEquals(attribute.getDisplayName(), attribute.getName());
        assertNotNull(attribute.getName());
        assertNotNull(attribute.getRTSArguments());
        assertNotNull(attribute.getRTSModuleName());
        assertNotNull(attribute.getApprovedUsage());
        assertNotNull(attribute.getRTSAttribute());
        assertNotNull(attribute.getCategory());
        assertNotNull(attribute.getDisplayName());


        attribute = table.getAttribute("Domain_Length");
        assertNotEquals(attribute.getDisplayName(), attribute.getName());
        assertNotNull(attribute.getName());
        assertNotNull(attribute.getRTSArguments());
        assertNotNull(attribute.getRTSModuleName());
        assertNotNull(attribute.getApprovedUsage());
        assertNotNull(attribute.getRTSAttribute());
        assertNotNull(attribute.getCategory());
        assertNotNull(attribute.getDisplayName());

        Map.Entry<Map<String, FieldSchema>, List<TransformDefinition>> datacomposition = table.getRealTimeTransformationMetadata();
        Set<String> fields = datacomposition.getKey().keySet();
        assertTrue(fields.contains("Domain_Length"));
        assertTrue(fields.contains("Title_Level"));
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
