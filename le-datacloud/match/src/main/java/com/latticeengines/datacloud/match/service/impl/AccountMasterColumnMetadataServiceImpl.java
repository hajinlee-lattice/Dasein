package com.latticeengines.datacloud.match.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.lang3.StringUtils;
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

    private AttributeRepository attrRepo;

    @Resource(name = "accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> accountMasterColumnService;

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
    protected String getLatestVersion() {
        return versionEntityMgr.currentApprovedVersion().getVersion();
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
            DataCloudVersion fullVersion = versionEntityMgr.findVersion(dataCloudVersion);
            String redshiftTableName = fullVersion.getAmBucketedRedShiftTable();
            if (StringUtils.isBlank(redshiftTableName)) {
                throw new RuntimeException("There is no redshift table for the data cloud version " + dataCloudVersion);
            }
            Map<TableRoleInCollection, String> tableNameMap = ImmutableMap.<TableRoleInCollection, String> builder()
                    .put(TableRoleInCollection.AccountMaster, redshiftTableName).build();
            Map<AttributeLookup, ColumnMetadata> cmMap = new HashMap<>();
            List<ColumnMetadata> amAttrs = fromPredefinedSelection(ColumnSelection.Predefined.Segment,
                    fullVersion.getVersion());
            amAttrs.forEach(cm -> cmMap.put(new AttributeLookup(BusinessEntity.LatticeAccount, cm.getName()), cm));
            ColumnMetadata idAttr = fromPredefinedSelection(ColumnSelection.Predefined.ID, fullVersion.getVersion())
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
