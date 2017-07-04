package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
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
    protected String getLatestVersion() {
        return versionEntityMgr.currentApprovedVersionAsString();
    }

    @Override
    protected AccountMasterColumn updateSavedMetadataColumn(String dataCloudVersion, ColumnMetadata columnMetadata) {
        AccountMasterColumn savedAccountMasterColumn = metadataColumnEntityMgr.findById(columnMetadata.getColumnId(),
                dataCloudVersion);

        savedAccountMasterColumn.setAmColumnId(columnMetadata.getColumnId());
        savedAccountMasterColumn.setDescription(columnMetadata.getDescription());
        savedAccountMasterColumn.setJavaClass(columnMetadata.getJavaClass());
        savedAccountMasterColumn.setDisplayName(columnMetadata.getDisplayName());
        savedAccountMasterColumn.setCategory(columnMetadata.getCategory());
        savedAccountMasterColumn.setSubcategory(columnMetadata.getSubcategory());
        savedAccountMasterColumn.setStatisticalType(columnMetadata.getStatisticalType());
        savedAccountMasterColumn.setFundamentalType(columnMetadata.getFundamentalType());
        savedAccountMasterColumn.setPremium(Boolean.TRUE.equals(columnMetadata.isPremium()));
        savedAccountMasterColumn.setDiscretizationStrategy(columnMetadata.getDiscretizationStrategy());
        savedAccountMasterColumn.setInternalEnrichment(Boolean.TRUE.equals(columnMetadata.isCanInternalEnrich()));

        ApprovedUsage approvedUsage = ApprovedUsage.NONE;
        if (Boolean.TRUE.equals(columnMetadata.isCanBis())) {
            approvedUsage = ApprovedUsage.MODEL_ALLINSIGHTS;
        } else if (Boolean.TRUE.equals(columnMetadata.isCanInsights())) {
            approvedUsage = ApprovedUsage.MODEL_MODELINSIGHTS;
        } else if (Boolean.TRUE.equals(columnMetadata.isCanModel())) {
            approvedUsage = ApprovedUsage.MODEL;
        }
        savedAccountMasterColumn.setApprovedUsage(approvedUsage);

        List<String> savedGroups = new ArrayList<>();
        for (String savedGroup : savedAccountMasterColumn.getGroups().split(",")) {
            savedGroups.add(savedGroup);
        }

        if (columnMetadata.isCanEnrich() && !savedGroups.contains(Predefined.Enrichment.name())) {
            savedGroups.add(Predefined.Enrichment.name());
        } else if (!columnMetadata.isCanEnrich() && savedGroups.contains(Predefined.Enrichment.name())) {
            savedGroups.remove(Predefined.Enrichment.name());
        }
        savedAccountMasterColumn.setGroups(StringUtils.join(savedGroups, ","));
        return savedAccountMasterColumn;
    }
}
