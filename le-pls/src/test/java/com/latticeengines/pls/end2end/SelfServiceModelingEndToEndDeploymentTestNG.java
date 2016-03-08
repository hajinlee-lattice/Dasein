package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
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
import org.springframework.http.MediaType;
import org.springframework.util.LinkedMultiValueMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.metadata.SemanticType;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.VdbMetadataField;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.AccessLevel;

public class SelfServiceModelingEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {
    @Autowired
    private WorkflowProxy workflowProxy;

    private static final Log log = LogFactory.getLog(SelfServiceModelingEndToEndDeploymentTestNG.class);
    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";
    private Tenant tenantToAttach;
    private SourceFile sourceFile;
    private String modelingWorkflowApplicationId;
    private String modelName;
    private ModelSummary originalModelSummary;

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
    }

    private void deleteTwoTenants() throws Exception {
        turnOffSslChecking();
        setTestingTenants();
        for (Tenant tenant : testingTenants) {
            deleteTenantByRestCall(tenant.getId());
        }
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment.lp", enabled = true)
    public void uploadFile() {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(RESOURCE_BASE + "/Mulesoft_SFDC_LP3_1000.csv"));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        ResponseDocument response = restTemplate.postForObject( //
                String.format("%s/pls/fileuploads/unnamed?schema=%s", getPLSRestAPIHostPort(),
                        SchemaInterpretation.LP3SalesforceLeadCSV), //
                requestEntity, ResponseDocument.class);
        sourceFile = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "uploadFile")
    public void resolveMetadata() {
        ResponseDocument response = restTemplate.getForObject(
                String.format("%s/pls/fileuploads/%s/metadata/unknown", getPLSRestAPIHostPort(), sourceFile.getName()),
                ResponseDocument.class);
        assertTrue(response.isSuccess());
        @SuppressWarnings("unchecked")
        List<Object> unknownColumns = new ObjectMapper().convertValue(response.getResult(), List.class);
        response = restTemplate.postForObject(
                String.format("%s/pls/fileuploads/%s/metadata/unknown", getPLSRestAPIHostPort(), sourceFile.getName()),
                unknownColumns, ResponseDocument.class);
        assertTrue(response.isSuccess());
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
        assertTrue(response.isSuccess());

        modelingWorkflowApplicationId = new ObjectMapper().convertValue(response.getResult(), String.class);

        System.out.println(String.format("Workflow application id is %s", modelingWorkflowApplicationId));
        waitForWorkflowStatus(modelingWorkflowApplicationId, true);

        response = restTemplate.postForObject(
                String.format("%s/pls/models/%s", getPLSRestAPIHostPort(), UUID.randomUUID()), parameters,
                ResponseDocument.class);
        assertFalse(response.isSuccess());

        WorkflowStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        assertEquals(completedStatus.getStatus(), BatchStatus.COMPLETED);
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
                SchemaInterpretation.LP3SalesforceLeadCSV.toString());
    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = "createModel")
    public void retrieveErrorsFile() {
        // Relies on error in Account.csv
        String csv = restTemplate.getForObject(
                String.format("%s/pls/fileuploads/%s/import/errors", getPLSRestAPIHostPort(), sourceFile.getName()),
                String.class);
        assertNotEquals(csv.length(), 0);
    }

    @Test(groups = "deployment.lp", enabled = true, dependsOnMethods = { "retrieveErrorsFile", "retrieveModelSummary",
            "retrieveReport" })
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
        assertTrue(response.isSuccess());

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
                return predictor.getName().equals(SemanticType.Website.toString())
                        || predictor.getName().equals(SemanticType.Country.toString());
            }

        }));
        assertEquals(found.getSourceSchemaInterpretation(), SchemaInterpretation.LP3SalesforceLeadCSV.toString());
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
                if (summary.getName().equals(modelName)) {
                    found = summary;
                }
            }
            if (found != null)
                break;
            Thread.sleep(1000);
        }
        return found;
    }

    private WorkflowStatus waitForWorkflowStatus(String applicationId, boolean running) {
        while (true) {
            WorkflowStatus status = workflowProxy.getWorkflowStatusFromApplicationId(applicationId);
            if (status == null) {
                continue;
            }
            if ((running && status.getStatus().isRunning()) || (!running && !status.getStatus().isRunning())) {
                return status;
            }
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private ModelingParameters createModelingParameters(String fileName) {
        ModelingParameters parameters = new ModelingParameters();
        parameters.setName("SelfServiceModelingEndToEndDeploymentTestNG_" + DateTime.now().getMillis());
        parameters.setDescription("Test");
        parameters.setFilename(fileName);
        return parameters;
    }
}
