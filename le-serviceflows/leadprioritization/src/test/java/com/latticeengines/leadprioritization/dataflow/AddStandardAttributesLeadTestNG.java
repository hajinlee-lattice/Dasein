package com.latticeengines.leadprioritization.dataflow;

import static org.junit.Assert.assertNotNull;
import static org.testng.Assert.assertFalse;
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
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

@ContextConfiguration(locations = { "classpath:serviceflows-leadprioritization-dataflow-context.xml" })
public class AddStandardAttributesLeadTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testAgainstStandardTransform() {
        AddStandardAttributesParameters parameters = new AddStandardAttributesParameters("EventTable",
                TransformationPipeline.getTransforms(TransformationGroup.STANDARD),
                SchemaInterpretation.SalesforceLead);
        Table table = executeDataFlow(parameters);
        System.out.println(JsonUtils.serialize(table));
        Attribute attribute = table.getAttribute("Industry_Group");
        assertNotEquals(attribute.getDisplayName(), attribute.getName());
        assertNotNull(attribute.getName());
        assertNotNull(attribute.getRTSArguments());
        assertNotNull(attribute.getRTSModuleName());
        assertNotNull(attribute.getApprovedUsage());
        assertNotNull(attribute.getRTSAttribute());
        assertNotNull(attribute.getCategory());
        assertNotNull(attribute.getDisplayName());

        attribute = table.getAttribute("ModelAction1");
        assertNotEquals(attribute.getDisplayName(), attribute.getName());
        assertNotNull(attribute.getName());
        assertNotNull(attribute.getRTSArguments());
        assertNotNull(attribute.getRTSModuleName());
        assertNotNull(attribute.getApprovedUsage());
        assertNotNull(attribute.getRTSAttribute());
        assertNotNull(attribute.getCategory());
        assertNotNull(attribute.getDisplayName());

        Map.Entry<Map<String, FieldSchema>, List<TransformDefinition>> datacomposition = table
                .getRealTimeTransformationMetadata();
        Set<String> fields = datacomposition.getKey().keySet();
        assertTrue(fields.contains("ModelAction1"));
        assertTrue(fields.contains("Industry_Group"));
        assertFalse(fields.contains("TitleRole"));
    }

    @Test(groups = "functional")
    public void testAgainstStandardAndPocTransform() {
        AddStandardAttributesParameters parameters = new AddStandardAttributesParameters("EventTable",
                TransformationPipeline.getTransforms(TransformationGroup.ALL), SchemaInterpretation.SalesforceLead);
        Table table = executeDataFlow(parameters);
        System.out.println(JsonUtils.serialize(table));
        Attribute attribute = table.getAttribute("Industry_Group");
        assertNotEquals(attribute.getDisplayName(), attribute.getName());
        assertNotNull(attribute.getName());
        assertNotNull(attribute.getRTSArguments());
        assertNotNull(attribute.getRTSModuleName());
        assertNotNull(attribute.getApprovedUsage());
        assertNotNull(attribute.getRTSAttribute());
        assertNotNull(attribute.getCategory());
        assertNotNull(attribute.getDisplayName());

        attribute = table.getAttribute("TitleRole");
        assertNotEquals(attribute.getDisplayName(), attribute.getName());
        assertNotNull(attribute.getName());
        assertNotNull(attribute.getRTSArguments());
        assertNotNull(attribute.getRTSModuleName());
        assertNotNull(attribute.getApprovedUsage());
        assertNotNull(attribute.getRTSAttribute());
        assertNotNull(attribute.getCategory());
        assertNotNull(attribute.getDisplayName());

        Map.Entry<Map<String, FieldSchema>, List<TransformDefinition>> datacomposition = table
                .getRealTimeTransformationMetadata();
        Set<String> fields = datacomposition.getKey().keySet();
        assertTrue(fields.contains("TitleRole"));
        assertTrue(fields.contains("Industry_Group"));
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
