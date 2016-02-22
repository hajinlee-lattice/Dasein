package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.pls.entitymanager.impl.microservice.WorkflowProxy;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;

public class SelfServiceModelingEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {
    @Autowired
    private WorkflowProxy workflowProxy;

    private static final Log log = LogFactory.getLog(SelfServiceModelingEndToEndDeploymentTestNG.class);
    private static final String RESOURCE_BASE = "com/latticeengines/pls/end2end/selfServiceModeling/csvfiles";
    private String tenant;
    private Tenant tenantToAttach;
    private CustomerSpace customerSpace;
    private SourceFile sourceFile;

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
        tenant = tenantToAttach.getId();
        customerSpace = CustomerSpace.parse(tenant);
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

    @Test(groups = "deployment.lp", enabled = true)
    public void uploadFile() {
        LinkedMultiValueMap<String, Object> map = new LinkedMultiValueMap<>();
        map.add("file", new ClassPathResource(RESOURCE_BASE + "/Account.csv"));
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        HttpEntity<LinkedMultiValueMap<String, Object>> requestEntity = new HttpEntity<>(map, headers);
        ResponseDocument response = restTemplate.postForObject( //
                String.format("%s/pls/fileuploads/unnamed?schema=%s", getPLSRestAPIHostPort(),
                        SchemaInterpretation.SalesforceAccount), //
                requestEntity, ResponseDocument.class);
        sourceFile = new ObjectMapper().convertValue(response.getResult(), SourceFile.class);
    }

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
        ModelingParameters parameters = createModelingParameters(sourceFile.getName());
        ResponseDocument response = restTemplate.postForObject(
                String.format("%s/pls/models/%s/model", getPLSRestAPIHostPort(), sourceFile.getName()), parameters,
                ResponseDocument.class);
        assertTrue(response.isSuccess());

        @SuppressWarnings("unchecked")
        String applicationId = new ObjectMapper().convertValue(response.getResult(), String.class);

        System.out.println(String.format("Workflow application id is %s", applicationId));
        waitForWorkflowStatus(applicationId, true);

        response = restTemplate.postForObject(
                String.format("%s/pls/models/%s/model", getPLSRestAPIHostPort(), sourceFile.getName()), parameters,
                ResponseDocument.class);
        assertFalse(response.isSuccess());

        WorkflowStatus completedStatus = waitForWorkflowStatus(applicationId, false);
        assertEquals(completedStatus.getStatus(), BatchStatus.COMPLETED);

        Job job = restTemplate.getForObject( //
                String.format("%s/pls/jobs/yarnapps/%s", getPLSRestAPIHostPort(), applicationId), //
                Job.class);
        assertNotNull(job);
        List<Report> reports = job.getReports();
        assertEquals(reports.size(), 1);
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
        parameters.setName("Test");
        parameters.setDescription("Test");
        parameters.setFilename(fileName);
        return parameters;
    }
}
