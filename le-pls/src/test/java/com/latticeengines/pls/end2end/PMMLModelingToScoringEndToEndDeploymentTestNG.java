package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    @Autowired
    protected InternalScoringApiInterface internalScoringApiProxy;

    @Autowired
    private WorkflowProxy workflowProxy;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        log.info("Bootstrapping test tenants using tenant console ...");
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
        tenantToAttach = testBed.getMainTestTenant();
        log.info("Test environment setup finished.");
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
                String.format("%s/pls/models/pmml/%s?module=%s&pivotfile=%s&pmmlfile=%s", getRestAPIHostPort(),
                        modelName, "module1", "pivotvalues.csv", "rfpmml.xml"), //
                null, ResponseDocument.class);
        String applicationId = new ObjectMapper().convertValue(response.getResult(), String.class);
        System.out.println(applicationId);
        waitForWorkflowStatus(applicationId, true);
        JobStatus completedStatus = waitForWorkflowStatus(applicationId, false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
        ModelSummary modelSummary = getModelSummary(modelName);
        modelId = modelSummary.getId();
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

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "createModel" })
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

}
