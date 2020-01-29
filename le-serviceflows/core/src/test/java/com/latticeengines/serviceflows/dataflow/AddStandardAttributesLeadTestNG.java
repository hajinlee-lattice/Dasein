package com.latticeengines.serviceflows.dataflow;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.AddStandardAttributesParameters;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

@ContextConfiguration(locations = { "classpath:serviceflows-core-dataflow-context.xml" })
public class AddStandardAttributesLeadTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testAgainstStandardTransform() {
        AddStandardAttributesParameters parameters = new AddStandardAttributesParameters("EventTable",
                TransformationPipeline.getTransforms(TransformationGroup.STANDARD),
                SchemaInterpretation.SalesforceLead);
        Table table = executeDataFlow(parameters);
        System.out.println(JsonUtils.serialize(table));
        Attribute attribute = table.getAttribute("Industry_Group");
        Assert.assertNotEquals(attribute.getDisplayName(), attribute.getName());
        Assert.assertNotNull(attribute.getName());
        Assert.assertNotNull(attribute.getRTSArguments());
        Assert.assertNotNull(attribute.getRTSModuleName());
        Assert.assertNotNull(attribute.getApprovedUsage());
        Assert.assertNotNull(attribute.getRTSAttribute());
        Assert.assertNotNull(attribute.getCategory());
        Assert.assertNotNull(attribute.getDisplayName());

        attribute = table.getAttribute("ModelAction1");
        Assert.assertNotEquals(attribute.getDisplayName(), attribute.getName());
        Assert.assertNotNull(attribute.getName());
        Assert.assertNotNull(attribute.getRTSArguments());
        Assert.assertNotNull(attribute.getRTSModuleName());
        Assert.assertNotNull(attribute.getApprovedUsage());
        Assert.assertNotNull(attribute.getRTSAttribute());
        Assert.assertNotNull(attribute.getCategory());
        Assert.assertNotNull(attribute.getDisplayName());

        Map.Entry<Map<String, FieldSchema>, List<TransformDefinition>> datacomposition = table
                .getRealTimeTransformationMetadata();
        Set<String> fields = datacomposition.getKey().keySet();
        Assert.assertTrue(fields.contains("ModelAction1"));
        Assert.assertTrue(fields.contains("Industry_Group"));
        Assert.assertFalse(fields.contains("TitleRole"));
    }

    @Test(groups = "functional")
    public void testAgainstStandardAndPocTransform() {
        AddStandardAttributesParameters parameters = new AddStandardAttributesParameters("EventTable",
                TransformationPipeline.getTransforms(TransformationGroup.ALL), SchemaInterpretation.SalesforceLead);
        Table table = executeDataFlow(parameters);
        System.out.println(JsonUtils.serialize(table));
        Attribute attribute = table.getAttribute("Industry_Group");
        Assert.assertNotEquals(attribute.getDisplayName(), attribute.getName());
        Assert.assertNotNull(attribute.getName());
        Assert.assertNotNull(attribute.getRTSArguments());
        Assert.assertNotNull(attribute.getRTSModuleName());
        Assert.assertNotNull(attribute.getApprovedUsage());
        Assert.assertNotNull(attribute.getRTSAttribute());
        Assert.assertNotNull(attribute.getCategory());
        Assert.assertNotNull(attribute.getDisplayName());

        attribute = table.getAttribute("TitleRole");
        Assert.assertNotEquals(attribute.getDisplayName(), attribute.getName());
        Assert.assertNotNull(attribute.getName());
        Assert.assertNotNull(attribute.getRTSArguments());
        Assert.assertNotNull(attribute.getRTSModuleName());
        Assert.assertNotNull(attribute.getApprovedUsage());
        Assert.assertNotNull(attribute.getRTSAttribute());
        Assert.assertNotNull(attribute.getCategory());
        Assert.assertNotNull(attribute.getDisplayName());

        Map.Entry<Map<String, FieldSchema>, List<TransformDefinition>> datacomposition = table
                .getRealTimeTransformationMetadata();
        Set<String> fields = datacomposition.getKey().keySet();
        Assert.assertTrue(fields.contains("TitleRole"));
        Assert.assertTrue(fields.contains("Industry_Group"));
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
