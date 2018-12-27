package com.latticeengines.admin.service.impl;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.security.Tenant;

public class AdminCleanupErrorTenantJobCallable implements Callable<Boolean> {
    private static final Logger log = LoggerFactory.getLogger(AdminCleanupErrorTenantJobCallable.class);
    private static final String customerBase = "/user/s-analytics/customers";

    @SuppressWarnings("unused")
    private String jobArguments;
    private com.latticeengines.admin.service.TenantService adminTenantService;
    private com.latticeengines.security.exposed.service.TenantService tenantService;
    private Configuration yarnConfiguration;
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String customerBucket;

    private Camille camille;
    private String podId;
    public AdminCleanupErrorTenantJobCallable(Builder builder)  {
        this.jobArguments = builder.jobArguments;
        this.adminTenantService = builder.adminTenantService;
        this.tenantService = builder.tenantService;
        this.yarnConfiguration = builder.yarnConfiguration;
        this.s3Service = builder.s3Service;
    }

    @Override
    public Boolean call() {
        camille = CamilleEnvironment.getCamille();
        podId = CamilleEnvironment.getPodId();

        List<Tenant> tenants = tenantService.getAllTenants();
        log.info("Scanning through " + tenants.size() + " tenants ...");
        for (Tenant tenant : tenants) {
            CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
            TenantDocument tenantDoc = adminTenantService.getTenant(customerSpace.getContractId(), customerSpace.getTenantId());
            if (tenantDoc.getBootstrapState().state == BootstrapState.State.ERROR) {
                log.info("Found a error tenant to clean up: " + tenant.getId());
                cleanupTenantInGA(tenant);
                cleanupTenantInZK(customerSpace.getContractId());
                cleanupTenantInHdfs(customerSpace.getContractId());
                cleanupS3(customerSpace.getTenantId());
            }
        }

        log.info("Finished cleaning up error tenants.");

        return null;
    }

    private void cleanupTenantInGA(Tenant tenant) {
        try {
            log.info("Clean up tenant in GA: " + tenant.getId());
            tenantService.discardTenant(tenant);
        } catch (Exception e) {
            log.error("Failed to clean up GA tenant " + tenant.getId(), e);
        }
    }

    private void cleanupTenantInZK(String contractId) {
        try {
            log.info("Clean up tenant in ZK: " + contractId);
            Path contractPath = PathBuilder.buildContractPath(podId, contractId);
            if (camille.exists(contractPath)) {
                camille.delete(contractPath);
            }
        } catch (Exception e) {
            log.error("Failed to clean up tenant in ZK: " + contractId, e);
        }
    }

    private void cleanupTenantInHdfs(String contractId) {
        try {
            log.info("Clean up contract in HDFS: " + contractId);
            String customerSpace = CustomerSpace.parse(contractId).toString();
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
        } catch (Exception e) {
            log.error("Failed to clean up contract in HDFS: " + contractId, e);
        }
    }

    private void cleanupS3(String tenantId) {
        log.info("Removing S3 folder: " + tenantId);
        try {
            s3Service.cleanupPrefix(customerBucket, tenantId);
        } catch (Exception e) {
            log.error("Failed to remove S3 folder: " + tenantId, e);
        }
    }

    public static class Builder {
        private String jobArguments;
        private com.latticeengines.admin.service.TenantService adminTenantService;
        private com.latticeengines.security.exposed.service.TenantService tenantService;
        private Configuration yarnConfiguration;
        private S3Service s3Service;

        public Builder() {

        }

        public Builder jobArguments(String jobArguments) {
            this.jobArguments = jobArguments;
            return this;
        }

        public Builder adminTenantService(com.latticeengines.admin.service.TenantService adminTenantService) {
            this.adminTenantService = adminTenantService;
            return this;
        }

        public Builder securityTenantService(com.latticeengines.security.exposed.service.TenantService tenantService) {
            this.tenantService = tenantService;
            return this;
        }

        public Builder yarnConfiguration(Configuration yarnConfiguration) {
            this.yarnConfiguration = yarnConfiguration;
            return this;
        }

        public Builder s3Service(S3Service s3Service) {
            this.s3Service = s3Service;
            return this;
        }
    }
}
