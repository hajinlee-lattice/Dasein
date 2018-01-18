package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.AttributeMap;
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
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.network.exposed.scoringapi.InternalScoringApiInterface;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class PMMLModelingToScoringEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/pmml";
    private static final String PMML_FILE_NAME = "rfpmml.xml";
    private static final String TEST_FILE_DISPLAY_NAME = "SelfServiceScoring Test File.csv";
    private static final Logger log = LoggerFactory.getLogger(PMMLModelingToScoringEndToEndDeploymentTestNG.class);
    private Tenant tenantToAttach;
    private String modelName = "pmmlmodel";
    private String modelId;
    private String fileName;

    private SourceFile sourceFile;

    @Autowired
    protected InternalScoringApiInterface internalScoringApiProxy;

    @Autowired
    private WorkflowProxy workflowProxy;

    private Long jobId;

    private String applicationId;

    private static final int TOTAL_QUALIFIED_LINES = 1126;

    private RestTemplate restTemplate;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        log.info("Bootstrapping test tenants using tenant console ...");
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
        tenantToAttach = testBed.getMainTestTenant();
        log.info("Test environment setup finished.");
        fileName = "Lattice_Relaunch_Small_NO_ID.csv";
        restTemplate = testBed.getRestTemplate();
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
                        getRestAPIHostPort(), modelName, "PMML MODEL", "module1", "pivotvalues.csv", PMML_FILE_NAME,
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

        activateModelSummary(modelId);
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
                job = workflowProxy.getWorkflowJobFromApplicationId(applicationId,
                        CustomerSpace.parse(mainTestTenant.getId()).toString());
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
        log.info(String.format("Score is %s", String.valueOf(response.getScore())));
    }

    private DebugScoreResponse score(Map<String, Object> record, String modelId) {
        ScoreRequest request = new ScoreRequest();
        request.setModelId(modelId);
        request.setRecord(record);
        DebugScoreResponse response = internalScoringApiProxy.scoreProbabilityRecord(request, tenantToAttach.getName(),
                false, false);
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
        ResponseDocument response = restTemplate.postForObject( //
                String.format("%s/pls/scores/fileuploads?modelId=%s&displayName=%s", getRestAPIHostPort(), modelId,
                        TEST_FILE_DISPLAY_NAME),
                requestEntity, ResponseDocument.class);
        assertTrue(response.isSuccess());
        sourceFile = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
        log.info(sourceFile.getName());
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "uploadTestingDataFile", enabled = true)
    public void scoreTestingData() throws Exception {
        System.out.println(String.format("%s/pls/scores/%s?fileName=%s&useRtsApi=TRUE", getRestAPIHostPort(), modelId,
                sourceFile.getName()));
        applicationId = restTemplate.postForObject(
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

    @Test(groups = "deployment.lp", dependsOnMethods = "scoreTestingData", timeOut = 600000)
    public void testJobIsListed() {
        final String jobType = "importAndRTSBulkScoreWorkflow";
        boolean any = false;
        while (true) {
            @SuppressWarnings("unchecked")
            List<Object> raw = restTemplate
                    .getForObject(String.format("%s/pls/scores/jobs/%s", getRestAPIHostPort(), modelId), List.class);
            List<Job> jobs = JsonUtils.convertList(raw, Job.class);
            any = Iterables.any(jobs, new Predicate<Job>() {

                @Override
                public boolean apply(@Nullable Job job) {
                    String jobModelId = job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
                    String testSourceFileName = job.getInputs()
                            .get(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME);
                    return job.getJobType() != null && job.getJobType().equals(jobType) && modelId.equals(jobModelId)
                            && TEST_FILE_DISPLAY_NAME.equals(testSourceFileName);
                }
            });

            if (any) {
                break;
            }
            sleep(500);
        }

        assertTrue(any);
    }

    private void sleep(long msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "testJobIsListed", timeOut = 1800000)
    public void poll() {
        JobStatus terminal;
        while (true) {
            Job job = restTemplate.getForObject(
                    String.format("%s/pls/jobs/yarnapps/%s", getRestAPIHostPort(), applicationId), Job.class);
            assertNotNull(job);
            jobId = job.getId();
            if (Job.TERMINAL_JOB_STATUS.contains(job.getJobStatus())) {
                terminal = job.getJobStatus();
                break;
            }
            sleep(1000);
        }

        assertEquals(terminal, JobStatus.COMPLETED);
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "poll")
    public void downloadCsv() throws IOException {
        restTemplate.getMessageConverters().add(new ByteArrayHttpMessageConverter());
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL));
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> response = restTemplate.exchange(
                String.format("%s/pls/scores/jobs/%d/results/score", getRestAPIHostPort(), jobId), HttpMethod.GET,
                entity, byte[].class);
        assertEquals(response.getStatusCode(), HttpStatus.OK);
        String results = new String(response.getBody());
        assertTrue(response.getHeaders().getFirst("Content-Disposition").contains("_scored.csv"));
        assertTrue(results.length() > 0);
        CSVParser parser = null;
        InputStream is = new ByteArrayInputStream(response.getBody());
        InputStreamReader reader = new InputStreamReader(is);
        CSVFormat format = LECSVFormat.format;
        try {
            parser = new CSVParser(reader, format);
            Set<String> csvHeaders = parser.getHeaderMap().keySet();
            assertTrue(csvHeaders.contains("CompanyName"));
            assertTrue(csvHeaders.contains("SourceColumn"));
            assertTrue(csvHeaders.contains("Score"));
            assertTrue(csvHeaders.contains("Event"));
            assertFalse(csvHeaders.contains("Rating"));

            int line = 1;
            for (CSVRecord record : parser.getRecords()) {
                assertTrue(StringUtils.isNotEmpty(record.get("Score")));
                line++;
            }
            assertEquals(line, TOTAL_QUALIFIED_LINES);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            parser.close();
        }
    }

    private void activateModelSummary(String modelId) {
        log.info("Update model " + modelId + " to active.");
        String modelApi = getRestAPIHostPort() + "/pls/modelsummaries/" + modelId;
        AttributeMap attrMap = new AttributeMap();
        attrMap.put("Status", ModelSummaryStatus.ACTIVE.getStatusCode());
        HttpEntity<AttributeMap> requestEntity = new HttpEntity<>(attrMap);
        restTemplate.exchange(modelApi, HttpMethod.PUT, requestEntity, Object.class);
        // Look up the model summary with details
        ModelSummary summary = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/%s", getRestAPIHostPort(), modelId), ModelSummary.class);
        assertEquals(summary.getStatus(), ModelSummaryStatus.ACTIVE);
    }

}
