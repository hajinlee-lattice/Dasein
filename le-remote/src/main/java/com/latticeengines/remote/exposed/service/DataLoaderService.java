package com.latticeengines.remote.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.admin.CreateVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.DeleteVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.GetVisiDBDLRequest;
import com.latticeengines.domain.exposed.dataloader.GetSpecRequest;
import com.latticeengines.domain.exposed.dataloader.GetSpecResult;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.dataloader.InstallTemplateRequest;
import com.latticeengines.domain.exposed.dataloader.LaunchJobsResult;
import com.latticeengines.domain.exposed.dataloader.QueryDataResult;
import com.latticeengines.domain.exposed.dataloader.QueryStatusResult;
import com.latticeengines.domain.exposed.dataplatform.visidb.GetQueryMetaDataColumnsRequest;
import com.latticeengines.domain.exposed.dataplatform.visidb.GetQueryMetaDataColumnsResponse;
import com.latticeengines.domain.exposed.pls.CrmConfig;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.pls.Segment;

public interface DataLoaderService {

    List<String> getSegmentNames(String tenantName, String dlUrl);

    List<Segment> getSegments(String tenantName, String dlUrl);

    InstallResult setSegments(String tenantName, String dlUrl, List<Segment> segments);

    InstallResult installVisiDBStructureFile(InstallTemplateRequest request, String dlUrl);

    InstallResult installDataLoaderConfigFile(InstallTemplateRequest request, String dlUrl);
    
    GetSpecResult getSpecDetails(GetSpecRequest request, String dlUrl);
    
    GetQueryMetaDataColumnsResponse getQueryMetadataColumns(GetQueryMetaDataColumnsRequest request, String dlUrl);

    String getTemplateVersion(String tenantName, String dlUrl);

    String getSfdcUser(String tenantName, String dlUrl);

    String getMarketoUserId(String tenantName, String dlUrl);

    String getMarketoUrl(String tenantName, String dlUrl);

    String getEloquaUsername(String tenantName, String dlUrl);

    String getEloquaCompany(String tenantName, String dlUrl);

    InstallResult getDLTenantSettings(GetVisiDBDLRequest getRequest, String dlUrl);

    InstallResult createDLTenant(CreateVisiDBDLRequest postRequest, String dlUrl);

    InstallResult deleteDLTenant(DeleteVisiDBDLRequest request, String dlUrl, boolean retry);

    void verifyCredentials(String crmType, CrmCredential crmCredential, boolean isProduction, String dlUrl);

    void updateDataProvider(String crmType, String plsTenantId, CrmConfig crmConfig, String dlUrl);

    long executeLoadGroup(String tenantName, String groupName, String dlUrl);

    LaunchJobsResult getLaunchJobs(long launchId, String dlUrl, boolean retry);

    String getLoadGroupLastSuccessTime(String tenantName, String groupName, String dlUrl);

    long getLastFailedLaunchId(String tenantName, String groupName, String dlUrl);

    boolean isLoadGroupRunning(String tenantName, String groupName, String dlUrl);

    InstallResult getLoadGroupStatus(String tenantName, String groupName, String dlUrl);

    InstallResult cancelLaunch(long launchId, String dlUrl);

    String runQuery(String tenantName, String queryName, String dlUrl);

    QueryStatusResult getQueryStatus(String tenantName, String queryHandle, String dlUrl);

    QueryDataResult getQueryData(String tenantName, String queryHandle, int startRow, int rowCount, String dlUrl);
}
