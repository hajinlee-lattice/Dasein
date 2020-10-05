package com.latticeengines.pls.service.impl;

import static com.latticeengines.domain.exposed.cdl.AtlasProfileReportStatus.Status.Generating;

import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.camille.exposed.locks.LockManager;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.AtlasProfileReportRequest;
import com.latticeengines.domain.exposed.cdl.AtlasProfileReportStatus;
import com.latticeengines.pls.service.ProfileReportService;
import com.latticeengines.proxy.exposed.cdl.ProfileReportProxy;

@Service
public class ProfileReportServiceImpl implements ProfileReportService {

    private static final Logger log = LoggerFactory.getLogger(ProfileReportServiceImpl.class);

    @Inject
    private ProfileReportProxy profileReportProxy;


    @Override
    public AtlasProfileReportStatus refreshProfile() {
        String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
        AtlasProfileReportStatus status = profileReportProxy.getProfileReportStatus(tenantId);
        if (Generating.equals(status.getStatus())) {
            log.info("There is already a profile report job running.");
            return status;
        } else {
            String lockName = getLockName(tenantId);
            try {
                LockManager.registerCrossDivisionLock(lockName);
                LockManager.acquireWriteLock(lockName, 10, TimeUnit.MINUTES);
                log.info("Won the distributed lock {}", lockName);
            } catch (Exception e) {
                log.warn("Error while acquiring zk lock {}", lockName, e);
            }
            try {
                status = profileReportProxy.getProfileReportStatus(tenantId);
                if (Generating.equals(status.getStatus())) {
                    log.info("There is already a profile report job running.");
                    return status;
                } else {
                    AtlasProfileReportRequest request = new AtlasProfileReportRequest();
                    request.setUserId(MultiTenantContext.getEmailAddress());
                    ApplicationId appId = profileReportProxy.generateProfileReport(tenantId, request);
                    log.info("Submitted profile report job, appId={}", appId);
                    AtlasProfileReportStatus toReturn = new AtlasProfileReportStatus();
                    toReturn.setStatus(Generating);
                    return toReturn;
                }
            } finally {
                LockManager.releaseWriteLock(lockName);
            }
        }
    }

    @Override
    public AtlasProfileReportStatus getStatus() {
        String tenantId = MultiTenantContext.getCustomerSpace().getTenantId();
        return profileReportProxy.getProfileReportStatus(tenantId);
    }

    private String getLockName(String tenantId) {
        return  "ProfileReport_" + tenantId;
    }

}
