package com.latticeengines.leadprioritization.wokflow;

import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.transform.TransformationMetadata;
import com.latticeengines.leadprioritization.workflow.ImportMatchAndModelWorkflowConfiguration;

import junit.framework.Assert;

public class ImportMatchAndModelWorkflowConfigurationUnitTestNG {

    private ImportMatchAndModelWorkflowConfiguration.Builder builder = new ImportMatchAndModelWorkflowConfiguration.Builder()
            .sourceSchemaInterpretation(SchemaInterpretation.SalesforceAccount.toString());

    @Test(groups = "unit")
    public void testGetTransformDefinitions() {
        List<TransformDefinition> transformDefinitions = builder.getTransformDefinitions(TransformationGroup.STANDARD);
        TransformDefinition definition = transformDefinitions.stream()
                .filter(def -> def.name.equals("StdVisidbDsIndustryGroup")).findFirst().orElse(null);
        TransformationMetadata tm = definition.transformationMetadata;
        Assert.assertNotNull(tm);
        Assert.assertEquals(definition.transformationMetadata.getCategory(), Category.ACCOUNT_INFORMATION.toString());
    }
}
