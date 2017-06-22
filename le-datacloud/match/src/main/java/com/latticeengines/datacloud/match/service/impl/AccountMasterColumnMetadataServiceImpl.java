package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("accountMasterColumnMetadataService")
public class AccountMasterColumnMetadataServiceImpl extends BaseColumnMetadataServiceImpl<AccountMasterColumn> {
    private static final Log log = LogFactory.getLog(AccountMasterColumnMetadataServiceImpl.class);

    private AttributeRepository attrRepo;

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
        Date cachedDate = cachedRefreshDate.get(approvedVersionValue);
        if (approvedVersionDate == null || (cachedDate != null && approvedVersionDate.compareTo(cachedDate) <= 0)) {
            log.info("Metadata Refresh Date : " + approvedVersionDate
                    + " Cache is already update-to-date as per the metadata refresh date so not refreshing it again");
            return false;
        }
        cachedRefreshDate.put(approvedVersionValue, approvedVersionDate);
        log.info("Metadata Refresh Date : " + approvedVersionDate + " Refreshing cache");
        return true;
    }

    @Override
    public AttributeRepository getAttrRepo(String dataCloudVersion) {
        if (attrRepo == null) {
            InputStream is = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("com/latticeengines/datacloud/match/amstats.json");
            ObjectMapper om = new ObjectMapper();
            Statistics statistics;
            try {
                statistics = om.readValue(is, Statistics.class);
            } catch (IOException e) {
                throw new RuntimeException("Failed parse amstats.json", e);
            }
            DataCloudVersion latestVersion = versionEntityMgr.latestApprovedForMajorVersion(dataCloudVersion);
            String redshiftTableName = latestVersion.getAmBucketedRedShiftTable();
            if (StringUtils.isBlank(redshiftTableName)) {
                throw new RuntimeException("There is no redshift table for the data cloud version " + latestVersion);
            }
            Map<TableRoleInCollection, String> tableNameMap = ImmutableMap.<TableRoleInCollection, String> builder()
                    .put(TableRoleInCollection.AccountMaster, redshiftTableName).build();
            Map<AttributeLookup, ColumnMetadata> cmMap = new HashMap<>();
            List<ColumnMetadata> amAttrs = fromPredefinedSelection(ColumnSelection.Predefined.Segment,
                    latestVersion.getVersion());
            amAttrs.forEach(cm -> cmMap.put(new AttributeLookup(BusinessEntity.LatticeAccount, cm.getName()), cm));
            ColumnMetadata idAttr = fromPredefinedSelection(ColumnSelection.Predefined.ID, latestVersion.getVersion())
                    .get(0);
            cmMap.put( //
                    new AttributeLookup(BusinessEntity.LatticeAccount, InterfaceName.LatticeAccountId.name()), //
                    idAttr);
            attrRepo = AttributeRepository.constructRepo(statistics, tableNameMap, cmMap,
                    CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE),
                    DataCloudConstants.ACCOUNT_MASTER_COLLECTION);
        }
        return attrRepo;
    }

}
