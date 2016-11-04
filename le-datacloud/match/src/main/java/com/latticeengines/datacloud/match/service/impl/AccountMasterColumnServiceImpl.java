package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.Resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

@Component("accountMasterColumnService")
public class AccountMasterColumnServiceImpl extends BaseMetadataColumnServiceImpl<AccountMasterColumn> {

    @Resource(name = "accountMasterColumnEntityMgr")
    private MetadataColumnEntityMgr<AccountMasterColumn> metadataColumnEntityMgr;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    private final ConcurrentMap<String, ConcurrentMap<String, AccountMasterColumn>> whiteColumnCache = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentSkipListSet<String>> blackColumnCache = new ConcurrentHashMap<>();

    @Override
    public boolean accept(String version) {
        return MatchUtils.isValidForAccountMasterBasedMatch(version);
    }

    @Override
    protected MetadataColumnEntityMgr<AccountMasterColumn> getMetadataColumnEntityMgr() {
        return metadataColumnEntityMgr;
    }

    @Override
    protected ConcurrentMap<String, ConcurrentMap<String, AccountMasterColumn>> getWhiteColumnCache() {
        return whiteColumnCache;
    }

    @Override
    protected ConcurrentMap<String, ConcurrentSkipListSet<String>> getBlackColumnCache() {
        return blackColumnCache;
    }

    @Override
    public List<AccountMasterColumn> findByColumnSelection(Predefined selectName, String dataCloudVersion) {
        return getMetadataColumnEntityMgr().findByTag(selectName.getName(), dataCloudVersion);
    }

    @Override
    protected List<String> getAllVersions() {
        List<DataCloudVersion> dataCloudVersions = versionEntityMgr.allVerions();
        List<String> versions = new ArrayList<>();
        for (DataCloudVersion dataCloudVersion : dataCloudVersions) {
            versions.add(dataCloudVersion.getVersion());
        }
        return versions;
    }

}
