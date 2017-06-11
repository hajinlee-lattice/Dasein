package com.latticeengines.pls.workflow;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.transform.TransformationMetadata;

public class ImportMatchAndModelWorkflowConfigurationUnitTestNG {

    @Test(groups = "unit")
    public void testGetTransformDefinitions() {
        List<TransformDefinition> transformDefinitions = UpdateTransformDefinitionsUtils.getTransformDefinitions(
                SchemaInterpretation.SalesforceAccount.toString(), TransformationGroup.STANDARD);
        TransformDefinition definition = transformDefinitions.stream()
                .filter(def -> def.name.equals("StdVisidbDsIndustryGroup")).findFirst().orElse(null);
        TransformationMetadata tm = definition.transformationMetadata;
        Assert.assertNotNull(tm);
        Assert.assertEquals(definition.transformationMetadata.getCategory(), Category.ACCOUNT_INFORMATION.toString());
    }
}
