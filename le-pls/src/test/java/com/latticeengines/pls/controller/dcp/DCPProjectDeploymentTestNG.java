package com.latticeengines.pls.controller.dcp;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.dcp.DCPProject;
import com.latticeengines.domain.exposed.dcp.DCPProjectDetails;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class DCPProjectDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String BASE_URL_PREFIX = "/pls/dcp/dcpproject";
    private static final String DISPLAY_NAME = "testProject";
    private static final String PROJECT_ID = "testProject";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        MultiTenantContext.setTenant(mainTestTenant);
    }

    @Test(groups = "deployment")
    public void testCreateDCPProjectWithProjectId() throws Exception {
        String url = String.format(BASE_URL_PREFIX + "?displayName=%s&projectId=%s&projectType=%s", DISPLAY_NAME, PROJECT_ID, DCPProject.ProjectType.Type1.name());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.valueOf("application/json;UTF-8"));
        HttpEntity<String> requestEntity = new HttpEntity<String>(headers);
        ResponseEntity<DCPProjectDetails> responseEntity = restTemplate.postForEntity(getRestAPIHostPort() + url, requestEntity,
                DCPProjectDetails.class);
        assertNotNull(responseEntity);
        DCPProjectDetails responseBody = responseEntity.getBody();
        assertEquals(responseBody.getProjectId(), PROJECT_ID);
        url = String.format(BASE_URL_PREFIX + "?projectId=%s", PROJECT_ID);
        restTemplate.delete(getRestAPIHostPort() + url);
    }

    @Test(groups = "deployment")
    public void testCreateDCPProjectWithOutProjectId() throws Exception {
        String url = String.format(BASE_URL_PREFIX + "?displayName=%s&projectType=%s", DISPLAY_NAME, DCPProject.ProjectType.Type1.name());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.valueOf("application/json;UTF-8"));
        HttpEntity<String> requestEntity = new HttpEntity<String>(headers);
        ResponseEntity<DCPProjectDetails> responseEntity = restTemplate.postForEntity(getRestAPIHostPort() + url, requestEntity,
                DCPProjectDetails.class);
        assertNotNull(responseEntity);
        DCPProjectDetails responseBody = responseEntity.getBody();
        assertEquals(responseBody.getProjectDisplayName(), DISPLAY_NAME);

        url = String.format(BASE_URL_PREFIX + "?projectId=%s", responseBody.getProjectId());
        restTemplate.delete(getRestAPIHostPort() + url);
    }

    @Test(groups = "deployment")
    public void testGetAllDCPProject() throws Exception {
        String url = String.format(BASE_URL_PREFIX + "?displayName=%s&projectType=%s", DISPLAY_NAME, DCPProject.ProjectType.Type1.name());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.valueOf("application/json;UTF-8"));
        HttpEntity<String> requestEntity = new HttpEntity<String>(headers);
        ResponseEntity<DCPProjectDetails> project1 = restTemplate.postForEntity(getRestAPIHostPort() + url, requestEntity, DCPProjectDetails.class);

        url = String.format(BASE_URL_PREFIX + "?displayName=%s&projectType=%s", DISPLAY_NAME, DCPProject.ProjectType.Type1.name());
        ResponseEntity<DCPProjectDetails> project2 = restTemplate.postForEntity(getRestAPIHostPort() + url, requestEntity, DCPProjectDetails.class);

        url = BASE_URL_PREFIX + "/list";
        List<?> projectList = restTemplate.getForObject(getRestAPIHostPort() + url, List.class);
        Assert.assertTrue(CollectionUtils.isNotEmpty(projectList));
        Assert.assertEquals(projectList.size(), 2);

        url = String.format(BASE_URL_PREFIX + "?projectId=%s", project1.getBody().getProjectId());
        restTemplate.delete(getRestAPIHostPort() + url);

        url = String.format(BASE_URL_PREFIX + "?projectId=%s", project2.getBody().getProjectId());
        restTemplate.delete(getRestAPIHostPort() + url);
    }
}
