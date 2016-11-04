package com.latticeengines.datacloud.match.service.impl;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;

@Component("accountMasterColumnMetadataService")
public class AccountMasterColumnMetadataServiceImpl extends BaseColumnMetadataServiceImpl<AccountMasterColumn> {

    @Resource(name = "accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> accountmasterColumnService;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForAccountMasterBasedMatch(version);
    }

    @Override
    protected MetadataColumnService<AccountMasterColumn> getMetadataColumnService() {
        return accountmasterColumnService;
    }

    @Override
    protected boolean isLatestVersion(String dataCloudVersion) {
        return getLatestVersion().equals(dataCloudVersion);
    }

    @Override
    protected String getLatestVersion() {
        return versionEntityMgr.currentApprovedVersion().getVersion();
    }

}
