package com.latticeengines.testframework.security.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-testframework-cleanup-context.xml",
        "classpath:camille-runtime-context.xml" })
public class GlobalAuthCleanupTestNG extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(GlobalAuthCleanupTestNG.class);
    private static final Long cleanupThreshold = TimeUnit.DAYS.toMillis(1);
    private static final String customerBase = "/user/s-analytics/customers";

    @Autowired
    private TenantService tenantService;

    @Autowired
    private Configuration yarnConfiguration;

    private Camille camille;
    private String podId;

    @BeforeClass(groups = "cleanup")
    public void setup() {
        camille = CamilleEnvironment.getCamille();
        podId = CamilleEnvironment.getPodId();
    }

    @Test(groups = "cleanup")
    public void cleanupTestTenants() throws Exception {
        List<Tenant> tenants = tenantService.getAllTenants();
        log.info("Scanning through " + tenants.size() + " tenants ...");
        for (Tenant tenant : tenants) {
            if (TestFrameworkUtils.isTestTenant(tenant)
                    && (System.currentTimeMillis() - tenant.getRegisteredTime()) > cleanupThreshold) {
                log.info("Found a test tenant to clean up: " + tenant.getId());
                cleanupTenantInGA(tenant);
                cleanupTenantInZK(tenant);
                cleanupTenantInHdfs(tenant);
            }
        }
        log.info("Finished cleaning up test tenants.");
    }

    private void cleanupTenantInGA(Tenant tenant) {
        log.info("Clean up tenant in GA: " + tenant.getId());
        tenantService.discardTenant(tenant);
    }

    private void cleanupTenantInZK(Tenant tenant) throws Exception {
        log.info("Clean up tenant in ZK: " + tenant.getId());
        String contractId = CustomerSpace.parse(tenant.getId()).getContractId();
        Path contractPath = PathBuilder.buildContractPath(podId, contractId);
        if (camille.exists(contractPath)) {
            camille.delete(contractPath);
        }
    }

    private void cleanupTenantInHdfs(Tenant tenant) throws Exception {
        log.info("Clean up tenant in HDFS: " + tenant.getId());
        String customerSpace = CustomerSpace.parse(tenant.getId()).toString();
        String contractId = CustomerSpace.parse(tenant.getId()).getContractId();

        String contractPath = PathBuilder.buildContractPath(podId, contractId).toString();
        if (HdfsUtils.fileExists(yarnConfiguration, contractPath)) {
            HdfsUtils.rmdir(yarnConfiguration, contractPath);
        }

        String customerPath = new Path(customerBase).append(customerSpace).toString();
        if (HdfsUtils.fileExists(yarnConfiguration, customerPath)) {
            HdfsUtils.rmdir(yarnConfiguration, customerPath);
        }
        contractPath = new Path(customerBase).append(contractId).toString();
        if (HdfsUtils.fileExists(yarnConfiguration, contractPath)) {
            HdfsUtils.rmdir(yarnConfiguration, contractPath);
        }
    }

}

