package com.latticeengines.remote.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.admin.CreateVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.DeleteVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.GetVisiDBDLRequest;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.dataloader.InstallTemplateRequest;
import com.latticeengines.domain.exposed.pls.CrmConfig;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.domain.exposed.pls.Segment;

public interface DataLoaderService {

    List<String> getSegmentNames(String tenantName, String dlUrl);

    List<Segment> getSegments(String tenantName, String dlUrl);

    InstallResult setSegments(String tenantName, String dlUrl, List<Segment> segments);

    InstallResult installVisiDBStructureFile(InstallTemplateRequest request, String dlUrl);

    InstallResult installDataLoaderConfigFile(InstallTemplateRequest request, String dlUrl);

    String getTemplateVersion(String tenantName, String dlUrl);

    String getSfdcUser(String tenantName, String dlUrl);

    String getMarketoUserId(String tenantName, String dlUrl);

    String getEloquaUsername(String tenantName, String dlUrl);

    String getEloquaCompany(String tenantName, String dlUrl);

    InstallResult getDLTenantSettings(GetVisiDBDLRequest getRequest, String dlUrl);

    InstallResult createDLTenant(CreateVisiDBDLRequest postRequest, String dlUrl);

    InstallResult deleteDLTenant(DeleteVisiDBDLRequest request, String dlUrl, boolean retry);

    void verifyCredentials(String crmType, CrmCredential crmCredential, boolean isProduction, String dlUrl);

    void updateDataProvider(String crmType, String plsTenantId, CrmConfig crmConfig, String dlUrl);

}
