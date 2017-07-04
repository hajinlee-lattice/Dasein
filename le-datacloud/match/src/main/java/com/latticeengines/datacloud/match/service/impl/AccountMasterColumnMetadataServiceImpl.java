package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.AMRelease;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.camille.exposed.watchers.WatcherCache;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;

@Component("accountMasterColumnMetadataService")
public class AccountMasterColumnMetadataServiceImpl extends BaseColumnMetadataServiceImpl<AccountMasterColumn> {

    private WatcherCache<String, AttributeRepository> attrRepoCache;

    @Resource(name = "accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> accountMasterColumnService;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;

    @PostConstruct
    private void postConstruct() {
        initCache();
    }

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
        return attrRepoCache.get(dataCloudVersion);
    }

    private AttributeRepository readAttrRepoFromHdfs(String dataCloudVersion) {
        DataCloudVersion fullVersion = versionEntityMgr.findVersion(dataCloudVersion);
        String redshiftTableName = fullVersion.getAmBucketedRedShiftTable();
        if (StringUtils.isBlank(redshiftTableName)) {
            throw new IllegalStateException(
                    "There is no redshift table for the data cloud version " + dataCloudVersion);
        }
        String statsVersion = fullVersion.getSegmentStatsVersion();
        if (StringUtils.isBlank(statsVersion)) {
            throw new IllegalStateException(
                    "There is not segment stats version for data cloud version " + dataCloudVersion);
        }
        String snapshotDir = hdfsPathBuilder.constructSnapshotDir("AccountMasterSegmentStats", statsVersion).toString();
        Iterator<GenericRecord> recordIterator = AvroUtils.iterator(yarnConfiguration, snapshotDir + "/*.avro");
        StatsCube cube = StatsCubeUtils.parseAvro(recordIterator);
        List<ColumnMetadata> amAttrs = fromPredefinedSelection(ColumnSelection.Predefined.Segment,
                fullVersion.getVersion());
        Statistics statistics = StatsCubeUtils.constructStatistics(cube, //
                Collections.singletonList(Pair.of(BusinessEntity.LatticeAccount, amAttrs)));
        Map<TableRoleInCollection, String> tableNameMap = ImmutableMap.<TableRoleInCollection, String> builder()
                .put(TableRoleInCollection.AccountMaster, redshiftTableName).build();
        Map<AttributeLookup, ColumnMetadata> cmMap = new HashMap<>();
        amAttrs.forEach(cm -> cmMap.put(new AttributeLookup(BusinessEntity.LatticeAccount, cm.getName()), cm));
        ColumnMetadata idAttr = fromPredefinedSelection(ColumnSelection.Predefined.ID, fullVersion.getVersion()).get(0);
        cmMap.put( //
                new AttributeLookup(BusinessEntity.LatticeAccount, InterfaceName.LatticeAccountId.name()), //
                idAttr);
        return AttributeRepository.constructRepo(statistics, tableNameMap, cmMap,
                CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE),
                DataCloudConstants.ACCOUNT_MASTER_COLLECTION);
    }

    @SuppressWarnings("unchecked")
    private void initCache() {
        attrRepoCache = WatcherCache.builder() //
                .name("AttrRepoCache") //
                .watch(AMRelease) //
                .maximum(10) //
                .load(dataCloudVersion -> readAttrRepoFromHdfs((String) dataCloudVersion)) //
                .initKeys(new String[] { versionEntityMgr.currentApprovedVersionAsString() }) //
                .build();
    }

}
