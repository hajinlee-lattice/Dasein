package com.latticeengines.pls.functionalframework;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.ProtectedRestApiProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

public class PlsDeploymentTestNGBase extends PlsAbstractTestNGBase {

    @Autowired
    @Qualifier(value = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @Value("${common.test.pls.url}")
    private String deployedHostPort;

    @PostConstruct
    private void postConstruct() {
        setTestBed(deploymentTestBed);
    }

    @Override
    protected String getRestAPIHostPort() {
        return getDeployedRestAPIHostPort();
    }

    protected String getDeployedRestAPIHostPort() {
        return deployedHostPort.endsWith("/") ? deployedHostPort.substring(0, deployedHostPort.length() - 1)
                : deployedHostPort;
    }

    protected void setupTestEnvironmentWithOneTenant()
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        turnOffSslChecking();
        testBed.bootstrap(1);
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
    }

    protected void setupTestEnvironmentWithOneTenantForProduct(LatticeProduct product)
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        turnOffSslChecking();
        testBed.bootstrapForProduct(product);
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
    }

    protected void setupTestEnvironmentWithOneTenantForProduct(LatticeProduct product,
            Map<String, Boolean> featureFlagMap) throws NoSuchAlgorithmException, KeyManagementException, IOException {
        turnOffSslChecking();
        testBed.bootstrapForProduct(product, featureFlagMap);
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
    }

    protected void deleteUserByRestCall(String username) {
        switchToSuperAdmin();
        String url = getRestAPIHostPort() + "/pls/users/\"" + username + "\"";
        restTemplate.delete(url);
    }

    protected void attachProtectedProxy(ProtectedRestApiProxy proxy) {
        proxy.attachInterceptor(((GlobalAuthDeploymentTestBed) testBed).getPlsAuthInterceptor());
        logger.info("Attached a " + proxy.getClass().getSimpleName() + " to pls auth interceptor.");
    }

    protected JobStatus waitForWorkflowStatus(WorkflowProxy workflowProxy, String applicationId, boolean running) {

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

}
