package com.latticeengines.datacloud.match.service.impl;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.util.MatchTypeUtil;

@Component("accountMasterColumnService")
public class AccountMasterColumnServiceImpl extends BaseMetadataColumnServiceImpl<AccountMasterColumn> {

    @Resource(name = "accountMasterColumnEntityMgr")
    private MetadataColumnEntityMgr<AccountMasterColumn> metadataColumnEntityMgr;

    @Value("${datacloud.match.latest.data.cloud.version:2.0.1}")
    private String latestDataCloudVersion;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    private final ConcurrentMap<String, AccountMasterColumn> whiteColumnCache = new ConcurrentHashMap<>();
    private final ConcurrentSkipListSet<String> blackColumnCache = new ConcurrentSkipListSet<>();

    @Override
    public boolean accept(String version) {
        return MatchTypeUtil.isValidForAccountMasterBasedMatch(version);
    }

    @Override
    protected MetadataColumnEntityMgr<AccountMasterColumn> getMetadataColumnEntityMgr() {
        return metadataColumnEntityMgr;
    }

    @Override
    protected ConcurrentMap<String, AccountMasterColumn> getWhiteColumnCache() {
        return whiteColumnCache;
    }

    @Override
    protected ConcurrentSkipListSet<String> getBlackColumnCache() {
        return blackColumnCache;
    }

    @Override
    public List<AccountMasterColumn> findByColumnSelection(Predefined selectName, String dataCloudVersion) {
        return getMetadataColumnEntityMgr().findByTag(selectName.getName(), dataCloudVersion);
    }

    @Override
    protected boolean isLatestVersion(String dataCloudVersion) {
        return getLatestVersion().equals(dataCloudVersion);
    }

    @Override
    protected String getLatestVersion() {
        return versionEntityMgr.latestApprovedForMajorVersion(latestDataCloudVersion).getVersion();
    }
}
