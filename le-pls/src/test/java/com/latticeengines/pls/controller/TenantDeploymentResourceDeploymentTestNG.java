package com.latticeengines.pls.controller;

import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.dataloader.JobStatus;
import com.latticeengines.domain.exposed.dataloader.LaunchJobsResult;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.pls.TenantDeployment;
import com.latticeengines.domain.exposed.pls.TenantDeploymentStatus;
import com.latticeengines.domain.exposed.pls.TenantDeploymentStep;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.remote.exposed.service.CrmCredentialZKService;
import com.latticeengines.security.exposed.Constants;

public class TenantDeploymentResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private CrmCredentialZKService crmCredentialZKService;
    
    private FeatureFlagDefinition def;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        switchToSuperAdmin();
        setupCamille(mainTestingTenant);
    }

    private void setupCamille(Tenant tenant) throws Exception {
        CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), customerSpace.getContractId(),
                customerSpace.getTenantId(), customerSpace.getSpaceId());
        path = path.append(new Path("/SpaceConfiguration"));
        if (!camille.exists(path)) {
            camille.create(path, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }
        path = path.append(new Path("/Topology"));
        if (camille.exists(path)) {
            camille.delete(path);
        }
        camille.create(path, new Document(CRMTopology.SFDC.name()), ZooDefs.Ids.OPEN_ACL_UNSAFE);

        def = FeatureFlagClient.getDefinition(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName());
        def.setConfigurable(true);
        FeatureFlagClient.setDefinition(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName(), def);
        FeatureFlagClient.setEnabled(customerSpace, LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName(), true);
    }

    @Test(groups = "deployment", enabled = false)
    public void runDeployment() {
        clearTemplateData();
        saveSfdcCredential();
        importAndEnrichData();
    }

    private void saveSfdcCredential() {
        CrmCredential crmCredential = new CrmCredential();
        crmCredential.setUserName("apeters-widgettech@lattice-engines.com");
        crmCredential.setPassword("Happy2010");
        crmCredential.setSecurityToken("oIogZVEFGbL3n0qiAp6F66TC");
        CrmCredential newCrmCredential = restTemplate.postForObject(getRestAPIHostPort()
                + "/pls/credentials/sfdc?tenantId=" +  mainTestingTenant.getId() +   "&isProduction=true&verifyOnly=false",
                crmCredential, CrmCredential.class);
        Assert.assertEquals(newCrmCredential.getOrgId(), "00D80000000KvZoEAK");

        CustomerSpace customerSpace = CustomerSpace.parse(mainTestingTenant.getId());
        FeatureFlagClient.removeFromSpace(customerSpace, LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName());
        def.setConfigurable(false);
        FeatureFlagClient.setDefinition(LatticeFeatureFlag.USE_EAI_VALIDATE_CREDENTIAL.getName(), def);
    }

    private void clearTemplateData() {
        restTemplate.delete(getRestAPIHostPort() + "/pls/tenantdeployments/deployment");
    }

    private void importAndEnrichData() {
        SimpleBooleanResponse response = restTemplate.postForObject(getRestAPIHostPort()
                + "/pls/tenantdeployments/importsfdcdata", null, SimpleBooleanResponse.class);
        Assert.assertTrue(response.isSuccess());

        HttpEntity<String> request = getRequest();
        String step = TenantDeploymentStep.IMPORT_SFDC_DATA.toString();
        String status = TenantDeploymentStatus.IN_PROGRESS.toString();
        String url = getRestAPIHostPort() + "/pls/tenantdeployments/objects?step=" + step + "&status=" + status;
        waitForLaunchCompletion(url, request);
        TenantDeployment deployment = getTenantDeployment(request);
        if (deployment.getStep().equals(TenantDeploymentStep.IMPORT_SFDC_DATA)) {
            Assert.assertEquals(deployment.getStatus(), TenantDeploymentStatus.SUCCESS);
        }

        step = TenantDeploymentStep.ENRICH_DATA.toString();
        url = getRestAPIHostPort() + "/pls/tenantdeployments/objects?step=" + step + "&status=" + status;
        waitForLaunchCompletion(url, request);
        deployment = getTenantDeployment(request);
        Assert.assertEquals(deployment.getStep(), TenantDeploymentStep.VALIDATE_METADATA);
        Assert.assertTrue(deployment.getStatus().equals(TenantDeploymentStatus.SUCCESS) ||
                deployment.getStatus().equals(TenantDeploymentStatus.WARNING));
    }

    private HttpEntity<String> getRequest() {
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        headers.add(Constants.INTERNAL_SERVICE_HEADERNAME, Constants.INTERNAL_SERVICE_HEADERVALUE);
        return new HttpEntity<>("", headers);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void waitForLaunchCompletion(String url, HttpEntity<String> request) {
        ResponseDocument<LaunchJobsResult> jobsResponse;
        ResponseEntity<ResponseDocument<LaunchJobsResult>> jobsResponseEntity;
        ParameterizedTypeReference jobsResponseType = new ParameterizedTypeReference<ResponseDocument<LaunchJobsResult>>() {};
        do {
            sleep();
            jobsResponseEntity = restTemplate.exchange(url, HttpMethod.GET, request, jobsResponseType);
            jobsResponse = jobsResponseEntity.getBody();
        } while (jobsResponse.isSuccess() && jobsResponse.getResult() != null &&
                !jobsResponse.getResult().getLaunchStatus().equals(JobStatus.SUCCESS));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private TenantDeployment getTenantDeployment(HttpEntity<String> request) {
        ResponseDocument<TenantDeployment> depResponse;
        ResponseEntity<ResponseDocument<TenantDeployment>> depResponseEntity;
        String url = getRestAPIHostPort() + "/pls/tenantdeployments/deployment";
        ParameterizedTypeReference depType = new ParameterizedTypeReference<ResponseDocument<TenantDeployment>>() {};
        depResponseEntity = restTemplate.exchange(url, HttpMethod.GET, request, depType);
        depResponse = depResponseEntity.getBody();
        Assert.assertTrue(depResponse.isSuccess());
        TenantDeployment deployment = depResponse.getResult();
        return deployment;
    }

    private void sleep() {
        try {
            Thread.sleep(5000);
        } catch (Exception exception) {
        }
    }
}
