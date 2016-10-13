package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.camille.exposed.config.ConfigurationController;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceScope;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.encryption.EncryptionGlobalState;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryStatus;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.service.TenantService;

@Component
public class SelfServiceModelingEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";
    private static final Log log = LogFactory.getLog(SelfServiceModelingEndToEndDeploymentTestNG.class);
    public static final String MODEL_DISPLAY_NAME = "Self Service Modeling Test Display Name";
    protected com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private ScoreCorrectnessService scoreCompareService;

    @Autowired
    private TenantService tenantService;

    private Tenant tenantToAttach;
    private SourceFile sourceFile;
    private String modelingWorkflowApplicationId;
    private String modelName;
    private ModelSummary originalModelSummary;
    private ModelSummary copiedModelSummary;
    private ModelSummary clonedModelSummary;
    private String fileName;
    private SchemaInterpretation schemaInterpretation = SchemaInterpretation.SalesforceLead;
    private ModelingParameters parameters;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        log.info("Bootstrapping test tenants using tenant console ...");
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
        tenantToAttach = testBed.getMainTestTenant();

        if (EncryptionGlobalState.isEnabled()) {
            ConfigurationController<CustomerSpaceScope> controller = ConfigurationController
                    .construct(new CustomerSpaceScope(CustomerSpace.parse(tenantToAttach.getId())));
            assertTrue(controller.exists(new Path("/EncryptionKey")));
        }
        // Create second tenant for copy model use case
        testBed.bootstrapForProduct(LatticeProduct.LPA3);
        log.info("Test environment setup finished.");
        saveAttributeSelection(CustomerSpace.parse(tenantToAttach.getName()));
        fileName = "Hootsuite_PLS132_LP3_ScoringLead_20160330_165806_modified.csv";

    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment.lp", enabled = true)
    public void uploadFile() {
        if (schemaInterpretation == null) {
            schemaInterpretation = SchemaInterpretation.SalesforceLead;
        }
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(RESOURCE_BASE + "/" + fileName));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        ResponseDocument response = restTemplate.postForObject( //
                String.format("%s/pls/models/uploadfile/unnamed?displayName=%s", getRestAPIHostPort(),
                        "SelfServiceModeling Test File.csv"), //
                requestEntity, ResponseDocument.class);
        sourceFile = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
        log.info(sourceFile.getName());

        map = new LinkedMultiValueMap<>();
        map.add("metadataFile", new ClassPathResource(
                "com/latticeengines/pls/end2end/selfServiceModeling/pivotmappingfiles/pivotvalues.txt.gz"));
        headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        requestEntity = new HttpEntity<>(map, headers);

        response = restTemplate.postForObject( //
                String.format("%s/pls/metadatauploads/modules/%s/%s?artifactName=%s&compressed=%s",
                        getRestAPIHostPort(), "module1", "pivotmappings", "pivotvalues", "true"), //
                requestEntity, ResponseDocument.class);
        String pivotFilePath = new ObjectMapper().convertValue(response.getResult(), String.class);
        log.info(pivotFilePath);

    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "uploadFile")
    public void resolveMetadata() {
        parameters = new ModelingParameters();
        parameters.setName("SelfServiceModelingEndToEndDeploymentTestNG_" + DateTime.now().getMillis());
        parameters.setDisplayName(MODEL_DISPLAY_NAME);
        parameters.setDescription("Test");
        parameters.setModuleName("module1");
        parameters.setPivotFileName("pivotvalues.csv");
        parameters.setFilename(sourceFile.getName());

        sourceFile.setSchemaInterpretation(schemaInterpretation);
        ResponseDocument response = restTemplate.postForObject(
                String.format("%s/pls/models/uploadfile/%s/fieldmappings?schema=%s", getRestAPIHostPort(),
                        sourceFile.getName(), schemaInterpretation.name()), parameters, ResponseDocument.class);
        FieldMappingDocument mappings = new ObjectMapper().convertValue(response.getResult(),
                FieldMappingDocument.class);

        for (FieldMapping mapping : mappings.getFieldMappings()) {
            if (mapping.getMappedField() == null) {
                mapping.setMappedToLatticeField(false);
                mapping.setMappedField(mapping.getUserField().replace(' ', '_'));
            }
            if (mapping.getMappedField().startsWith("Activity_Count")) {
                mapping.setFieldType(UserDefinedType.NUMBER);
            }
        }

        List<String> ignored = new ArrayList<>();
        ignored.add("Activity_Count_Interesting_Moment_Webinar");
        mappings.setIgnoredFields(ignored);

        log.info("the fieldmappings are: " + mappings.getFieldMappings());
        log.info("the ignored fields are: " + mappings.getIgnoredFields());
        restTemplate.postForObject(
                String.format("%s/pls/models/uploadfile/fieldmappings?displayName=%s", getRestAPIHostPort(),
                        sourceFile.getName()), mappings, Void.class);
    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "resolveMetadata")
    public void createModel() {
        modelName = parameters.getName();
        model(parameters);
    }

    @SuppressWarnings("rawtypes")
    private void model(ModelingParameters parameters) {
        ResponseDocument response;
        response = restTemplate.postForObject(
                String.format("%s/pls/models/%s", getRestAPIHostPort(), parameters.getName()), parameters,
                ResponseDocument.class);
        modelingWorkflowApplicationId = new ObjectMapper().convertValue(response.getResult(), String.class);

        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));
        waitForWorkflowStatus(modelingWorkflowApplicationId, true);

        boolean thrown = false;
        try {
            response = restTemplate.postForObject(
                    String.format("%s/pls/models/%s", getRestAPIHostPort(), UUID.randomUUID()), parameters,
                    ResponseDocument.class);
        } catch (Exception e) {
            thrown = true;
        }

        assertTrue(thrown);

        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "createModel", enabled = true)
    public void retrieveReport() {
        Job job = restTemplate.getForObject( //
                String.format("%s/pls/jobs/yarnapps/%s", getRestAPIHostPort(), modelingWorkflowApplicationId), //
                Job.class);
        assertNotNull(job);
        List<Report> reports = job.getReports();
        assertEquals(reports.size(), 2);
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "createModel", timeOut = 120000, enabled = true)
    public void retrieveModelSummary() throws InterruptedException {
        originalModelSummary = getModelSummary(modelName);
        assertNotNull(originalModelSummary);
        assertEquals(originalModelSummary.getSourceSchemaInterpretation(),
                SchemaInterpretation.SalesforceLead.toString());
        assertNotNull(originalModelSummary.getTrainingTableName());
        assertFalse(originalModelSummary.getTrainingTableName().isEmpty());
        assertJobExistsWithModelIdAndModelName(originalModelSummary.getId());
        // Inspect some predictors
        String rawModelSummary = originalModelSummary.getDetails().getPayload();
        JsonNode modelSummaryJson = JsonUtils.deserialize(rawModelSummary, JsonNode.class);
        JsonNode predictors = modelSummaryJson.get("Predictors");
        for (int i = 0; i < predictors.size(); ++i) {
            JsonNode predictor = predictors.get(i);
            assertNotEquals(predictor.get("Name"), "Activity_Count_Interesting_Moment_Webinar");
            if (predictor.get("Name") != null && predictor.get("Name").asText() != null) {
                if (predictor.get("Name").asText().equals("Some_Column")) {
                    JsonNode tags = predictor.get("Tags");
                    assertEquals(tags.size(), 1);
                    assertEquals(tags.get(0).textValue(), ModelingMetadata.INTERNAL_TAG);
                    assertEquals(predictor.get("Category").textValue(), ModelingMetadata.CATEGORY_LEAD_INFORMATION);
                } else if (predictor.get("Name").asText().equals("Industry")) {
                    JsonNode approvedUsages = predictor.get("ApprovedUsage");
                    assertEquals(approvedUsages.size(), 1);
                    assertEquals(approvedUsages.get(0).textValue(), ApprovedUsage.MODEL_ALLINSIGHTS.toString());
                }
            }
        }
    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "createModel")
    public void retrieveErrorsFile() {
        // Relies on error in Account.csv
        restTemplate.getMessageConverters().add(new ByteArrayHttpMessageConverter());
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL));
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> response = restTemplate.exchange(
                String.format("%s/pls/fileuploads/%s/import/errors", getRestAPIHostPort(), sourceFile.getName()),
                HttpMethod.GET, entity, byte[].class);
        assertEquals(response.getStatusCode(), HttpStatus.OK);
        String errors = new String(response.getBody());
        assertTrue(errors.length() > 0);
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "retrieveModelSummary")
    public void copyModel() {
        ResponseDocument response = getRestTemplate().postForObject(
                String.format("%s/pls/models/copymodel/%s?targetTenantId=%s", getRestAPIHostPort(),
                        originalModelSummary.getId(), testBed.getTestTenants().get(1).getId()), //
                null, ResponseDocument.class);
        Boolean res = new ObjectMapper().convertValue(response.getResult(), Boolean.class);
        assertTrue(res);
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "copyModel", timeOut = 1200000, enabled = true)
    public void retrieveModelSummaryForCopiedModel() throws InterruptedException, IOException {
        tenantToAttach = testBed.getTestTenants().get(1);
        testBed.switchToSuperAdmin(tenantToAttach);
        copiedModelSummary = getModelSummary(modelName);
        assertNotNull(copiedModelSummary);
        assertEquals(copiedModelSummary.getSourceSchemaInterpretation(),
                originalModelSummary.getSourceSchemaInterpretation());
        assertNotNull(copiedModelSummary.getTrainingTableName());
        assertFalse(copiedModelSummary.getTrainingTableName().isEmpty());
        // Inspect some predictors
        String rawModelSummary = copiedModelSummary.getDetails().getPayload();
        JsonNode modelSummaryJson = JsonUtils.deserialize(rawModelSummary, JsonNode.class);
        JsonNode predictors = modelSummaryJson.get("Predictors");
        for (int i = 0; i < predictors.size(); ++i) {
            JsonNode predictor = predictors.get(i);
            if (predictor.get("Name") != null && predictor.get("Name").asText() != null) {
                if (predictor.get("Name").asText().equals("Some_Column")) {
                    JsonNode tags = predictor.get("Tags");
                    assertEquals(tags.size(), 1);
                    assertEquals(tags.get(0).textValue(), ModelingMetadata.INTERNAL_TAG);
                    assertEquals(predictor.get("Category").textValue(), ModelingMetadata.CATEGORY_LEAD_INFORMATION);
                    assertNotEquals(predictor.get("Name"), "Activity_Count_Interesting_Moment_Webinar");
                } else if (predictor.get("Name").asText().equals("Industry")) {
                    JsonNode approvedUsages = predictor.get("ApprovedUsage");
                    assertEquals(approvedUsages.size(), 1);
                    assertEquals(approvedUsages.get(0).textValue(), ApprovedUsage.MODEL_ALLINSIGHTS.toString());
                }
            }
        }
        scoreCompareService.analyzeScores(tenantToAttach.getId(), RESOURCE_BASE + "/" + fileName,
                copiedModelSummary.getId(), 687);

    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = { "retrieveModelSummaryForCopiedModel" })
    public void cloneAndRemodel() {
        @SuppressWarnings("unchecked")
        List<Object> rawFields = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/metadata/%s", getRestAPIHostPort(), copiedModelSummary.getId()),
                List.class);
        List<VdbMetadataField> fields = new ArrayList<>();
        for (Object rawField : rawFields) {
            VdbMetadataField field = JsonUtils.convertValue(rawField, VdbMetadataField.class);
            fields.add(field);

            if (field.getColumnName().equals("Phone_Entropy")) {
                field.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
            }
            if (field.getColumnName().equals("Activity_Count_Click_Email")) {
                field.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
            }
        }

        // Now remodel
        CloneModelingParameters parameters = new CloneModelingParameters();
        parameters.setName(modelName + "_clone");
        modelName = parameters.getName();
        parameters.setDisplayName(MODEL_DISPLAY_NAME);
        parameters.setDescription("clone");
        parameters.setAttributes(fields);
        parameters.setSourceModelSummaryId(copiedModelSummary.getId());
        parameters.setDeduplicationType(DedupType.ONELEADPERDOMAIN);
        parameters.setEnableTransformations(true);
        parameters.setExcludePropDataAttributes(true);

        ResponseDocument<?> response;
        response = restTemplate.postForObject(String.format("%s/pls/models/%s/clone", getRestAPIHostPort(), modelName),
                parameters, ResponseDocument.class);

        modelingWorkflowApplicationId = new ObjectMapper().convertValue(response.getResult(), String.class);

        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));

        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "cloneAndRemodel", timeOut = 120000)
    public void retrieveModelSummaryForClonedModel() throws InterruptedException, IOException {
        clonedModelSummary = getModelSummary(modelName);
        assertNotNull(clonedModelSummary);
        assertJobExistsWithModelIdAndModelName(clonedModelSummary.getId());
        List<Predictor> predictors = clonedModelSummary.getPredictors();
        assertTrue(!Iterables.any(predictors, new Predicate<Predictor>() {
            @Override
            public boolean apply(@Nullable Predictor predictor) {
                return predictor.getName().equals(InterfaceName.Website.toString())
                        || predictor.getName().equals(InterfaceName.Country.toString());
            }

        }));
        assertEquals(clonedModelSummary.getSourceSchemaInterpretation(), SchemaInterpretation.SalesforceLead.toString());
        String foundFileTableName = clonedModelSummary.getTrainingTableName();
        assertNotNull(foundFileTableName);

        @SuppressWarnings("unchecked")
        List<Object> rawFields = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/metadata/%s", getRestAPIHostPort(), clonedModelSummary.getId()),
                List.class);
        assertTrue(Iterables.any(rawFields, new Predicate<Object>() {
            @Override
            public boolean apply(@Nullable Object raw) {
                VdbMetadataField metadataField = new ObjectMapper().convertValue(raw, VdbMetadataField.class);
                return metadataField.getColumnName().equals("Phone_Entropy")
                        && metadataField.getApprovedUsage().equals(ModelingMetadata.NONE_APPROVED_USAGE);
            }
        }));
        assertTrue(Iterables.any(rawFields, new Predicate<Object>() {
            @Override
            public boolean apply(@Nullable Object raw) {
                VdbMetadataField metadataField = new ObjectMapper().convertValue(raw, VdbMetadataField.class);
                return metadataField.getColumnName().equals("Activity_Count_Click_Email")
                        && metadataField.getApprovedUsage().equals(ModelingMetadata.NONE_APPROVED_USAGE);
            }
        }));

        scoreCompareService.analyzeScores(tenantToAttach.getId(), RESOURCE_BASE + "/" + fileName,
                clonedModelSummary.getId(), 843);
    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "retrieveModelSummaryForClonedModel", timeOut = 600000)
    public void scoreTrainingDataOfClonedModel() throws InterruptedException, IOException {
        System.out.println(String.format("%s/pls/scores/%s/training?useRtsApi=TRUE&performEnrichment=TRUE",
                getRestAPIHostPort(), clonedModelSummary.getId()));
        String applicationId = getRestTemplate().postForObject(
                String.format("%s/pls/scores/%s/training?useRtsApi=TRUE&performEnrichment=TRUE", getRestAPIHostPort(),
                        clonedModelSummary.getId()), //
                null, String.class);
        applicationId = StringUtils.substringBetween(applicationId.split(":")[1], "\"");
        System.out.println(String.format("Score training data applicationId = %s", applicationId));
        assertNotNull(applicationId);
        testJobIsListed("rtsBulkScoreWorkflow", clonedModelSummary.getId(), applicationId);
    }

    private void testJobIsListed(final String jobType, final String modelId, String applicationId) {
        boolean any = false;
        while (true) {
            @SuppressWarnings("unchecked")
            List<Object> raw = getRestTemplate().getForObject(
                    String.format("%s/pls/scores/jobs/%s", getRestAPIHostPort(), modelId), List.class);
            List<Job> jobs = JsonUtils.convertList(raw, Job.class);
            any = Iterables.any(jobs, new Predicate<Job>() {

                @Override
                public boolean apply(@Nullable Job job) {
                    String jobModelId = job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
                    String jobModelName = job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME);
                    return job.getJobType() != null && job.getJobType().equals(jobType) && modelId.equals(jobModelId)
                            && SelfServiceModelingEndToEndDeploymentTestNG.MODEL_DISPLAY_NAME.equals(jobModelName);
                }
            });

            if (any) {
                break;
            }
            sleep(500);
        }

        assertTrue(any);

        JobStatus terminal;
        while (true) {
            Job job = getRestTemplate().getForObject(
                    String.format("%s/pls/jobs/yarnapps/%s", getRestAPIHostPort(), applicationId), Job.class);
            assertNotNull(job);
            if (Job.TERMINAL_JOB_STATUS.contains(job.getJobStatus())) {
                terminal = job.getJobStatus();
                break;
            }
            sleep(1000);
        }
        assertEquals(terminal, JobStatus.COMPLETED);
    }

    private void sleep(long msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void saveAttributeSelection(CustomerSpace customerSpace) {
        internalResourceRestApiProxy = new com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy(
                getRestAPIHostPort());
        LeadEnrichmentAttributesOperationMap selectedAttributeMap = checkSelection(customerSpace);
        System.out.println(selectedAttributeMap.getDeselectedAttributes());
        System.out.println(selectedAttributeMap.getSelectedAttributes());
        internalResourceRestApiProxy.saveLeadEnrichmentAttributes(customerSpace, selectedAttributeMap);
    }

    private LeadEnrichmentAttributesOperationMap checkSelection(CustomerSpace customerSpace) {
        List<LeadEnrichmentAttribute> enrichmentAttributeList = internalResourceRestApiProxy
                .getLeadEnrichmentAttributes(customerSpace, null, null, false);
        LeadEnrichmentAttributesOperationMap selectedAttributeMap = new LeadEnrichmentAttributesOperationMap();
        List<String> selectedAttributes = new ArrayList<>();
        selectedAttributeMap.setSelectedAttributes(selectedAttributes);
        List<String> deselectedAttributes = new ArrayList<>();
        selectedAttributeMap.setDeselectedAttributes(deselectedAttributes);
        int premiumSelectCount = 2;
        int selectCount = 1;

        for (LeadEnrichmentAttribute attr : enrichmentAttributeList) {
            if (attr.getIsPremium()) {
                if (premiumSelectCount > 0) {
                    premiumSelectCount--;
                    selectedAttributes.add(attr.getFieldName());
                }
            } else {
                if (selectCount > 0) {
                    selectCount--;
                    selectedAttributes.add(attr.getFieldName());
                }
            }
        }

        return selectedAttributeMap;
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

        @SuppressWarnings("unchecked")
        List<Object> predictors = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/predictors/all/%s", getRestAPIHostPort(), found.getId()),
                List.class);
        assertTrue(Iterables.any(predictors, new Predicate<Object>() {

            @Override
            public boolean apply(@Nullable Object raw) {
                Predictor predictor = new ObjectMapper().convertValue(raw, Predictor.class);
                return predictor.getCategory() != null;
            }
        }));

        // Look up the model summary with details
        Object rawSummary = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/%s", getRestAPIHostPort(), found.getId()), Object.class);
        return JsonUtils.convertValue(rawSummary, ModelSummary.class);
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

    public void assertJobExistsWithModelIdAndModelName(final String jobModelId) {
        log.info(String.format("The model_id is: %s", jobModelId));
        @SuppressWarnings("unchecked")
        List<Object> rawJobs = restTemplate
                .getForObject(String.format("%s/pls/jobs", getRestAPIHostPort()), List.class);
        List<Job> jobs = JsonUtils.convertList(rawJobs, Job.class);
        assertTrue(Iterables.any(jobs, new Predicate<Job>() {
            @Override
            public boolean apply(@Nullable Job job) {
                return job.getOutputs().get(WorkflowContextConstants.Inputs.MODEL_ID).equals(jobModelId)
                        && job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME)
                                .equals(MODEL_DISPLAY_NAME);
            }
        }));
    }

    public String prepareModel(SchemaInterpretation schemaInterpretation, String fileName) throws InterruptedException {
        if (!StringUtils.isBlank(fileName)) {
            this.fileName = fileName;
        }
        if (schemaInterpretation != null) {
            this.schemaInterpretation = schemaInterpretation;
        }

        log.info("Uploading File");
        uploadFile();
        sourceFile = getSourceFile();
        log.info(sourceFile.getName());
        log.info("Resolving Metadata");
        resolveMetadata();
        log.info("Creating Model");
        createModel();
        retrieveModelSummary();
        ModelSummary modelSummary = getModelSummary();
        String modelId = modelSummary.getId();
        log.info("modeling id: " + modelId);
        return modelId;
    }

    public SourceFile getSourceFile() {
        return sourceFile;
    }

    public RestTemplate getRestTemplate() {
        return restTemplate;
    }

    public Tenant getTenant() {
        return tenantToAttach;
    }

    public ModelSummary getModelSummary() {
        return originalModelSummary;
    }

}
