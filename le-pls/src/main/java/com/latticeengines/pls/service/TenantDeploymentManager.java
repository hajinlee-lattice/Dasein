package com.latticeengines.pls.service;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.domain.exposed.dataloader.LaunchJobsResult;
import com.latticeengines.domain.exposed.dataloader.QueryStatusResult;
import com.latticeengines.domain.exposed.pls.TenantDeployment;
import com.latticeengines.domain.exposed.pls.TenantDeploymentStep;

public interface TenantDeploymentManager {

    void importSfdcData(String teanntId, TenantDeployment deployment);

    void enrichData(String tenantId, TenantDeployment deployment);

    void validateMetadata(String tenantId, TenantDeployment deployment);

    void cancelLaunch(String tenantId, long launchId);

    LaunchJobsResult getRunningJobs(String tenantId, TenantDeploymentStep step);

    LaunchJobsResult getCompleteJobs(String tenantId, TenantDeploymentStep step);

    String getStepSuccessTime(String tenantId, TenantDeploymentStep step);

    String runQuery(String tenantId, TenantDeploymentStep step);

    QueryStatusResult getQueryStatus(String tenantId, String queryHandle);

    void downloadQueryDataFile(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String tenantId, String queryHandle, String fileName);
}
