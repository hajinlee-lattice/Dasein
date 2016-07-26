package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Field;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.network.exposed.scoringapi.InternalScoringApiInterface;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class PMMLModelingToScoringEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/pmml";
    private static final Log log = LogFactory.getLog(PMMLModelingToScoringEndToEndDeploymentTestNG.class);
    private Tenant tenantToAttach;
    private String modelName = "pmmlmodel";
    private String modelId;
    private String fileName;

    private SourceFile sourceFile;

    @Autowired
    protected InternalScoringApiInterface internalScoringApiProxy;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private SelfServiceModelingEndToEndDeploymentTestNG selfServiceModeling;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        log.info("Bootstrapping test tenants using tenant console ...");
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
        tenantToAttach = testBed.getMainTestTenant();
        log.info("Test environment setup finished.");
        fileName = "Lattice_Relaunch_Small.csv";
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment.lp", enabled = true)
    public void uploadFile() {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("metadataFile", new ClassPathResource(RESOURCE_BASE + "/pivotvalues.txt"));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        ResponseDocument response = restTemplate.postForObject( //
                String.format("%s/pls/metadatauploads/modules/%s/%s?artifactName=%s", getRestAPIHostPort(), "module1",
                        "pivotmappings", "pivotvalues"), //
                requestEntity, ResponseDocument.class);
        String pivotFilePath = new ObjectMapper().convertValue(response.getResult(), String.class);
        System.out.println(pivotFilePath);

        map = new LinkedMultiValueMap<>();
        map.add("metadataFile", new ClassPathResource(RESOURCE_BASE + "/rfpmml.xml.gz"));
        headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        requestEntity = new HttpEntity<>(map, headers);
        response = restTemplate.postForObject( //
                String.format("%s/pls/metadatauploads/modules/%s/%s?artifactName=%s&compressed=%s",
                        getRestAPIHostPort(), "module1", "pmmlfiles", "rfpmml", "true"), //
                requestEntity, ResponseDocument.class);
        String pmmlFilePath = new ObjectMapper().convertValue(response.getResult(), String.class);
        System.out.println(pmmlFilePath);
    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "uploadFile")
    public void createModel() throws InterruptedException {
        @SuppressWarnings("rawtypes")
        ResponseDocument response = restTemplate.postForObject( //
                String.format("%s/pls/models/pmml/%s?displayname=%s&module=%s&pivotfile=%s&pmmlfile=%s&schema=%s",
                        getRestAPIHostPort(), modelName, "PMML MODEL", "module1", "pivotvalues.csv", "rfpmml.xml",
                        SchemaInterpretation.SalesforceLead), //
                null, ResponseDocument.class);
        String applicationId = new ObjectMapper().convertValue(response.getResult(), String.class);
        System.out.println(applicationId);
        waitForWorkflowStatus(applicationId, true);
        JobStatus completedStatus = waitForWorkflowStatus(applicationId, false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
        ModelSummary modelSummary = getModelSummary(modelName);
        modelId = modelSummary.getId();
        assertEquals(modelSummary.getSourceSchemaInterpretation(), SchemaInterpretation.SalesforceLead.toString());
        assertNotNull(modelSummary.getPivotArtifactPath());
    }

    private ModelSummary getModelSummary(String modelName) throws InterruptedException {
        ModelSummary found = null;
        // Wait for model downloader
        while (true) {
            @SuppressWarnings("unchecked")
            List<Object> summaries = restTemplate.getForObject( //
                    String.format("%s/pls/modelsummaries", getRestAPIHostPort()), List.class);
            for (Object rawSummary : summaries) {
                ModelSummary summary = new ObjectMapper().convertValue(rawSummary, ModelSummary.class);
                if (summary.getName().contains(modelName)) {
                    found = summary;
                }
            }
            if (found != null)
                break;
            Thread.sleep(1000);
        }
        assertNotNull(found);
        assertEquals(found.getStatus(), ModelSummaryStatus.INACTIVE);
        return found;
    }

    private JobStatus waitForWorkflowStatus(String applicationId, boolean running) {

        int retryOnException = 4;
        Job job = null;

        while (true) {
            try {
                job = workflowProxy.getWorkflowJobFromApplicationId(applicationId);
            } catch (Exception e) {
                System.out.println(String.format("Workflow job exception: %s", e.getMessage()));

                job = null;
                if (--retryOnException == 0)
                    throw new RuntimeException(e);
            }

            if ((job != null) && ((running && job.isRunning()) || (!running && !job.isRunning()))) {
                return job.getJobStatus();
            }

            try {
                Thread.sleep(30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "createModel")
    public void scoreRecords() throws IOException, InterruptedException {
        Map<String, Object> record = new HashMap<>();
        Fields fields = internalScoringApiProxy.getModelFields(modelId, tenantToAttach.getName());
        for (Field field : fields.getFields()) {
            record.put(field.getFieldName(), "1");
        }
        record.put("PD_DA_JobTitle", "_Part_Time");
        DebugScoreResponse response = score(record, modelId);
        System.out.print(response.getScore());
    }

    private DebugScoreResponse score(Map<String, Object> record, String modelId) {
        ScoreRequest request = new ScoreRequest();
        request.setModelId(modelId);
        request.setRecord(record);
        DebugScoreResponse response = internalScoringApiProxy.scoreProbabilityRecord(request, tenantToAttach.getName());
        return response;
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment.lp", dependsOnMethods = "scoreRecords")
    public void uploadTestingDataFile() {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(RESOURCE_BASE + "/" + fileName));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        ResponseDocument response = selfServiceModeling.getRestTemplate().postForObject( //
                String.format("%s/pls/scores/fileuploads?modelId=%s&displayName=%s", getRestAPIHostPort(), modelId,
                        "SelfServiceScoring Test File.csv"), requestEntity, ResponseDocument.class);
        assertTrue(response.isSuccess());
        sourceFile = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
        log.info(sourceFile.getName());
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "uploadTestingDataFile", enabled = true)
    public void testScoreTestingData() throws Exception {
        System.out.println(String.format("%s/pls/scores/%s?fileName=%s&useRtsApi=TRUE&performEnrichment=TRUE",
                getRestAPIHostPort(), modelId, sourceFile.getName()));
        String applicationId = selfServiceModeling.getRestTemplate().postForObject(
                String.format("%s/pls/scores/%s?fileName=%s&useRtsApi=TRUE", getRestAPIHostPort(), modelId,
                        sourceFile.getName()), //
                null, String.class);
        applicationId = StringUtils.substringBetween(applicationId.split(":")[1], "\"");
        System.out.println(String.format("Score testing data applicationId = %s", applicationId));
        assertNotNull(applicationId);
        waitForWorkflowStatus(applicationId, true);
        JobStatus completedStatus = waitForWorkflowStatus(applicationId, false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

}
