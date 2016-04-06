package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.springframework.batch.core.BatchStatus;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.AccessLevel;

@Component
public class SelfServiceModelingEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {
    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private SourceFileService sourceFileService;

    private static final Log log = LogFactory.getLog(SelfServiceModelingEndToEndDeploymentTestNG.class);
    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";
    private Tenant tenantToAttach;
    private SourceFile sourceFile;
    private String modelingWorkflowApplicationId;
    private String modelName;
    private ModelSummary originalModelSummary;
    private String fileName;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {

        System.out.println("Deleting existing test tenants ...");
        deleteTwoTenants();

        System.out.println("Bootstrapping test tenants using tenant console ...");
        setupTestEnvironment("pd", true);

        System.out.println("Setting up testing users ...");
        tenantToAttach = testingTenants.get(1);
        if (tenantToAttach.getName().contains("Tenant 1")) {
            tenantToAttach = testingTenants.get(0);
        }
        UserDocument doc = loginAndAttach(AccessLevel.SUPER_ADMIN, tenantToAttach);
        useSessionDoc(doc);

        log.info("Test environment setup finished.");
        System.out.println("Test environment setup finished.");

        fileName = "Mulesoft_SFDC_LP3_1000.csv";
    }

    private void deleteTwoTenants() throws Exception {
        turnOffSslChecking();
        setTestingTenants();
        for (Tenant tenant : testingTenants) {
            deleteTenantByRestCall(tenant.getId());
        }
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public SourceFile getSourceFile() {
        return sourceFile;
    }

    public RestTemplate getRestTemplate() {
        return restTemplate;
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment.lp", enabled = true)
    public void uploadFile() {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(RESOURCE_BASE + "/" + fileName));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        ResponseDocument response = restTemplate.postForObject( //
                String.format("%s/pls/models/fileuploads/unnamed?schema=%s&displayName=%s", getPLSRestAPIHostPort(),
                        SchemaInterpretation.SalesforceLead, "SelfServiceModeling Test File.csv"), //
                requestEntity, ResponseDocument.class);
        sourceFile = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
        System.out.println(sourceFile.getName());
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "uploadFile")
    public void resolveMetadata() {
        ResponseDocument response = restTemplate.getForObject(
                String.format("%s/pls/models/fileuploads/%s/metadata/unknown", getPLSRestAPIHostPort(),
                        sourceFile.getName()), ResponseDocument.class);
        @SuppressWarnings("unchecked")
        List<Object> unknownColumns = new ObjectMapper().convertValue(response.getResult(), List.class);
        response = restTemplate.postForObject(
                String.format("%s/pls/models/fileuploads/%s/metadata/unknown", getPLSRestAPIHostPort(),
                        sourceFile.getName()), unknownColumns, ResponseDocument.class);
    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "resolveMetadata")
    public void createModel() {
        ModelingParameters parameters = new ModelingParameters();
        parameters.setName("SelfServiceModelingEndToEndDeploymentTestNG_" + DateTime.now().getMillis());
        parameters.setDescription("Test");
        parameters.setFilename(sourceFile.getName());
        modelName = parameters.getName();
        model(parameters);
    }

    @SuppressWarnings("rawtypes")
    private void model(ModelingParameters parameters) {
        ResponseDocument response;
        response = restTemplate.postForObject(
                String.format("%s/pls/models/%s", getPLSRestAPIHostPort(), parameters.getName()), parameters,
                ResponseDocument.class);
        modelingWorkflowApplicationId = new ObjectMapper().convertValue(response.getResult(), String.class);

        System.out.println(String.format("Workflow application id is %s", modelingWorkflowApplicationId));
        waitForWorkflowStatus(modelingWorkflowApplicationId, true);

        boolean thrown = false;
        try {
            response = restTemplate.postForObject(
                    String.format("%s/pls/models/%s", getPLSRestAPIHostPort(), UUID.randomUUID()), parameters,
                    ResponseDocument.class);
        } catch (Exception e) {
            thrown = true;
        }

        assertTrue(thrown);

        WorkflowStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        assertEquals(completedStatus.getStatus(), BatchStatus.COMPLETED);
    }

    public Tenant getTenant() {
        return tenantToAttach;
    }

    public ModelSummary getModelSummary() {
        return originalModelSummary;
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "createModel")
    public void retrieveReport() {
        Job job = restTemplate.getForObject( //
                String.format("%s/pls/jobs/yarnapps/%s", getPLSRestAPIHostPort(), modelingWorkflowApplicationId), //
                Job.class);
        assertNotNull(job);
        List<Report> reports = job.getReports();
        assertEquals(reports.size(), 1);
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "createModel", timeOut = 120000)
    public void retrieveModelSummary() throws InterruptedException {
        originalModelSummary = getModelSummary(modelName);
        assertNotNull(originalModelSummary);
        assertEquals(originalModelSummary.getSourceSchemaInterpretation(),
                SchemaInterpretation.SalesforceLead.toString());
        assertNotNull(originalModelSummary.getTrainingTableName());
    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "createModel")
    public void retrieveErrorsFile() {
        // Relies on error in Account.csv
        restTemplate.getMessageConverters().add(new ByteArrayHttpMessageConverter());
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL));
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> response = restTemplate.exchange(
                String.format("%s/pls/models/fileuploads/%s/import/errors", getPLSRestAPIHostPort(),
                        sourceFile.getName()), HttpMethod.GET, entity, byte[].class);
        assertEquals(response.getStatusCode(), HttpStatus.OK);
        String errors = new String(response.getBody());
        assertTrue(errors.length() > 0);
    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = { "retrieveModelSummary" })
    public void cloneAndRemodel() {
        @SuppressWarnings("unchecked")
        List<Object> rawFields = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/metadata/%s", getPLSRestAPIHostPort(),
                        originalModelSummary.getId()), List.class);
        List<VdbMetadataField> fields = new ArrayList<>();
        for (Object rawField : rawFields) {
            VdbMetadataField field = new ObjectMapper().convertValue(rawField, VdbMetadataField.class);
            fields.add(field);

            if (field.getColumnName().equals("Website")) {
                field.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
            }
            if (field.getColumnName().equals("City")) {
                field.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
            }
        }

        // Now remodel
        CloneModelingParameters parameters = new CloneModelingParameters();
        parameters.setName(modelName + "_clone");
        modelName = parameters.getName();
        parameters.setDescription("clone");
        parameters.setAttributes(fields);
        parameters.setSourceModelSummaryId(originalModelSummary.getId());

        ResponseDocument response;
        response = restTemplate.postForObject(
                String.format("%s/pls/models/%s/clone", getPLSRestAPIHostPort(), modelName), parameters,
                ResponseDocument.class);

        modelingWorkflowApplicationId = new ObjectMapper().convertValue(response.getResult(), String.class);

        System.out.println(String.format("Workflow application id is %s", modelingWorkflowApplicationId));

        WorkflowStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        assertEquals(completedStatus.getStatus(), BatchStatus.COMPLETED);
    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "cloneAndRemodel", timeOut = 120000)
    public void retrieveModelSummaryForClonedModel() throws InterruptedException {
        ModelSummary found = getModelSummary(modelName);
        assertNotNull(found);
        List<Predictor> predictors = found.getPredictors();
        assertTrue(!Iterables.any(predictors, new Predicate<Predictor>() {
            @Override
            public boolean apply(@Nullable Predictor predictor) {
                return predictor.getName().equals(InterfaceName.Website.toString())
                        || predictor.getName().equals(InterfaceName.Country.toString());
            }

        }));
        assertEquals(found.getSourceSchemaInterpretation(), SchemaInterpretation.SalesforceLead.toString());
        String foundFileTableName = found.getTrainingTableName();
        assertNotNull(foundFileTableName);
    }

    private ModelSummary getModelSummary(String modelName) throws InterruptedException {
        ModelSummary found = null;
        // Wait for model downloader
        while (true) {
            @SuppressWarnings("unchecked")
            List<Object> summaries = restTemplate.getForObject( //
                    String.format("%s/pls/modelsummaries", getPLSRestAPIHostPort()), List.class);
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

        @SuppressWarnings("unchecked")
        List<Object> predictors = restTemplate.getForObject(
                String.format("%s/pls/modelsummaries/predictors/all/%s", getPLSRestAPIHostPort(), found.getId()),
                List.class);
        assertTrue(Iterables.any(predictors, new Predicate<Object>() {

            @Override
            public boolean apply(@Nullable Object raw) {
                Predictor predictor = new ObjectMapper().convertValue(raw, Predictor.class);
                return predictor.getCategory() != null;
            }
        }));
        return found;
    }

    private WorkflowStatus waitForWorkflowStatus(String applicationId, boolean running) {
        int retryOnException = 4;
        WorkflowStatus status = null;

        while (true) {
            try {
                status = workflowProxy.getWorkflowStatusFromApplicationId(applicationId);
            } catch (Exception e) {
                System.out.println(String.format("Workflow status exception: %s", e.getMessage()));

                status = null;
                if (--retryOnException == 0)
                    throw new RuntimeException(e);
            }

            if ((status != null)
                    && ((running && status.getStatus().isRunning()) || (!running && !status.getStatus().isRunning()))) {
                return status;
            }

            try {
                Thread.sleep(30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
