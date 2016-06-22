package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertNotEquals;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class ImportMatchAndModelWorkflowDeploymentTestNG extends ImportMatchAndModelWorkflowDeploymentTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/workflowapi/flows/leadprioritization/csvfiles";

    @Autowired
    private Configuration yarnConfiguration;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupForWorkflow();
    }

    @Test(groups = "deployment", enabled = true)
    public void modelSmallAccountData() throws Exception {
        SourceFile sourceFile = uploadFile(RESOURCE_BASE + "/AccountSmallData.csv",
                SchemaInterpretation.SalesforceAccount);
        ModelingParameters params = new ModelingParameters();
        params.setFilename(sourceFile.getName());
        params.setName("testWorkflowAccount");
        params.setTransformationGroup(TransformationGroup.STANDARD);
        String summary = getModelSummary(params.getName());
        JsonNode json = JsonUtils.deserialize(summary, JsonNode.class);
        JsonNode percentiles = json.get("PercentileBuckets");
        assertNotEquals(percentiles.size(), 0);
    }

    @Test(groups = "deployment", enabled = true)
    public void modelLead() throws Exception {
        SourceFile sourceFile = uploadFile(RESOURCE_BASE + "/Lead.csv", SchemaInterpretation.SalesforceLead);
        ModelingParameters params = new ModelingParameters();
        params.setFilename(sourceFile.getName());
        params.setName("testWorkflowLead");
        params.setTransformationGroup(TransformationGroup.STANDARD);
        model(params);
    }

}
