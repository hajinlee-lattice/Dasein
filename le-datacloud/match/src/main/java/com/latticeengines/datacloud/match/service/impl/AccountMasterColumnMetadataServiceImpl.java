package com.latticeengines.datacloud.match.service.impl;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Resource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;

@Component("accountMasterColumnMetadataService")
public class AccountMasterColumnMetadataServiceImpl extends BaseColumnMetadataServiceImpl<AccountMasterColumn> {
    private static final Log log = LogFactory.getLog(AccountMasterColumnMetadataServiceImpl.class);

    @Resource(name = "accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> accountMasterColumnService;

    private ConcurrentMap<String, Date> cachedRefreshDate = new ConcurrentHashMap<>();

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForAccountMasterBasedMatch(version);
    }

    @Override
    protected MetadataColumnService<AccountMasterColumn> getMetadataColumnService() {
        return accountMasterColumnService;
    }

    @Override
    protected boolean isLatestVersion(String dataCloudVersion) {
        return getLatestVersion().equals(dataCloudVersion);
    }

    @Override
    protected String getLatestVersion() {
        return versionEntityMgr.currentApprovedVersion().getVersion();
    }

    @Override
    protected boolean refreshCacheNeeded() {
        DataCloudVersion approvedVersion = versionEntityMgr.currentApprovedVersion();
        String approvedVersionValue = approvedVersion.getVersion();
        Date approvedVersionDate = approvedVersion.getMetadataRefreshDate();
        if (cachedRefreshDate.get(approvedVersionValue) != null
                && approvedVersionDate
                        .compareTo(cachedRefreshDate.get(approvedVersionValue)) <= 0) {
            log.info("Metadata Refresh Date : " + approvedVersionDate);
            log.info("Cache is already update-to-date as per the metadata refresh date so not refreshing it again");
            return false;
        }
        cachedRefreshDate.put(approvedVersionValue, approvedVersionDate);
        log.info("Metadata Refresh Date : " + approvedVersionDate);
        log.info("Cache refreshed successfully");
        return true;
    }

}
