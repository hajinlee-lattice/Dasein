package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.ModelNotes;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.pls.service.ModelNotesService;

public class ImportMatchAndModelWorkflowDeploymentTestNG extends ImportMatchAndModelWorkflowDeploymentTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/workflowapi/flows/leadprioritization/csvfiles";

    @Autowired
    private ModelNotesService modelNotesService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupForWorkflow();
    }

    @AfterClass(groups = "deployment")
    public void cleanup() throws Exception {
        cleanUpAfterWorkflow();
    }

    @Test(groups = "deployment", enabled = true)
    public void modelSmallAccountData() throws Exception {
        SourceFile sourceFile = uploadFile(RESOURCE_BASE + "/AccountSmallDataUpdated.csv",
                SchemaInterpretation.SalesforceAccount);
        ModelingParameters params = new ModelingParameters();
        params.setFilename(sourceFile.getName());
        params.setName("testWorkflowAccount");
        params.setTransformationGroup(TransformationGroup.STANDARD);
        params.setNotesContent("this is a test case!");
        params.setUserId("lpl@lattice-engines.com");
        model(params);
        String summary = getModelSummary(params.getName());
        JsonNode json = JsonUtils.deserialize(summary, JsonNode.class);
        JsonNode percentiles = json.get("PercentileBuckets");
        assertNotEquals(percentiles.size(), 0);

        ModelSummary modelSummary = locateModelSummary("testWorkflowAccount", DEMO_CUSTOMERSPACE);
        List<ModelNotes> list = modelNotesService.getAllByModelSummaryId(modelSummary.getId());
        assertEquals(list.size(), 1);
        ModelNotes note = list.get(0);
        assertEquals(note.getNotesContents(), "this is a test case!");
    }

    @Test(groups = "deployment", enabled = false)
    public void modelLead() throws Exception {
        SourceFile sourceFile = uploadFile(RESOURCE_BASE + "/Lead.csv", SchemaInterpretation.SalesforceLead);
        ModelingParameters params = new ModelingParameters();
        params.setFilename(sourceFile.getName());
        params.setName("testWorkflowLead");
        params.setTransformationGroup(TransformationGroup.STANDARD);
        model(params);
    }

}
