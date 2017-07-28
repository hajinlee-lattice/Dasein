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
import org.apache.commons.lang3.tuple.ImmutablePair;
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
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;

@Component("accountMasterColumnMetadataService")
public class AccountMasterColumnMetadataServiceImpl extends BaseColumnMetadataServiceImpl<AccountMasterColumn> {

    private WatcherCache<String, AttributeRepository> attrRepoCache;
    private WatcherCache<String, Pair<StatsCube, TopNTree>> statsCache;

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

    @Override
    public StatsCube getStatsCube(String dataCloudVersion) {
        return statsCache.get(dataCloudVersion).getLeft();
    }

    @Override
    public TopNTree getTopNTree(String dataCloudVersion) {
        return statsCache.get(dataCloudVersion).getRight();
    }

    private AttributeRepository readAttrRepoFromHdfs(String dataCloudVersion) {
        Statistics statistics = readStatisticsFromHdfs(dataCloudVersion, ColumnSelection.Predefined.Segment);
        DataCloudVersion fullVersion = versionEntityMgr.findVersion(dataCloudVersion);
        String redshiftTableName = fullVersion.getAmBucketedRedShiftTable();
        if (StringUtils.isBlank(redshiftTableName)) {
            throw new IllegalStateException(
                    "There is no redshift table for the data cloud version " + dataCloudVersion);
        }
        Map<TableRoleInCollection, String> tableNameMap = ImmutableMap.<TableRoleInCollection, String> builder()
                .put(TableRoleInCollection.AccountMaster, redshiftTableName).build();
        Map<AttributeLookup, ColumnMetadata> cmMap = new HashMap<>();
        List<ColumnMetadata> amAttrs = fromPredefinedSelection(ColumnSelection.Predefined.Segment,
                fullVersion.getVersion());
        Map<String, GenericRecord> profileMap = readProfileFromHdfs(dataCloudVersion);
        amAttrs.forEach(cm -> {
            if (profileMap.containsKey(cm.getColumnId())) {
                GenericRecord profileRecord = profileMap.get(cm.getColumnId());
                if (profileRecord.get(DataCloudConstants.PROFILE_ATTR_NUMBITS) != null) {
                    cm.setNumBits((int) profileRecord.get(DataCloudConstants.PROFILE_ATTR_NUMBITS));
                    cm.setBitOffset((int) profileRecord.get(DataCloudConstants.PROFILE_ATTR_LOWESTBIT));
                    cm.setPhysicalName(profileRecord.get(DataCloudConstants.PROFILE_ATTR_ENCATTR).toString());
                }
            }
            cmMap.put(new AttributeLookup(BusinessEntity.LatticeAccount, cm.getName()), cm);
        });
        ColumnMetadata idAttr = fromPredefinedSelection(ColumnSelection.Predefined.ID, fullVersion.getVersion()).get(0);
        cmMap.put( //
                new AttributeLookup(BusinessEntity.LatticeAccount, InterfaceName.LatticeAccountId.name()), //
                idAttr);
        return AttributeRepository.constructRepo(statistics, tableNameMap, cmMap,
                CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE),
                DataCloudConstants.ACCOUNT_MASTER_COLLECTION);
    }

    private Pair<StatsCube, TopNTree> readStatsPairFromHdfs(String dataCloudVersion) {
        Statistics statistics = readStatisticsFromHdfs(dataCloudVersion, ColumnSelection.Predefined.Enrichment);
        StatsCube statsCube = StatsCubeUtils.toStatsCube(statistics);
        TopNTree topNTree = StatsCubeUtils.toTopNTree(statistics);
        return ImmutablePair.of(statsCube, topNTree);
    }

    private Statistics readStatisticsFromHdfs(String dataCloudVersion, ColumnSelection.Predefined predefined) {
        DataCloudVersion fullVersion = versionEntityMgr.findVersion(dataCloudVersion);
        String statsVersion = getStatsVersion(dataCloudVersion, predefined);
        String sourceName = ColumnSelection.Predefined.Segment.equals(predefined) ? "AccountMasterSegmentStats" : "AccountMasterEnrichmentStats";
        String snapshotDir = hdfsPathBuilder.constructSnapshotDir(sourceName, statsVersion).toString();
        Iterator<GenericRecord> recordIterator = AvroUtils.iterator(yarnConfiguration, snapshotDir + "/*.avro");
        StatsCube cube = StatsCubeUtils.parseAvro(recordIterator);
        List<ColumnMetadata> amAttrs = fromPredefinedSelection(predefined, fullVersion.getVersion());
        return StatsCubeUtils.constructStatistics(cube, Collections.singletonList(Pair.of(BusinessEntity.LatticeAccount, amAttrs)));
    }

    private Map<String, GenericRecord> readProfileFromHdfs(String dataCloudVersion) {
        String statsVersion = getStatsVersion(dataCloudVersion, ColumnSelection.Predefined.Segment);
        String snapshotDir = hdfsPathBuilder.constructSnapshotDir("AccountMasterProfile", statsVersion).toString();
        Iterator<GenericRecord> recordIterator = AvroUtils.iterator(yarnConfiguration, snapshotDir + "/*.avro");
        Map<String, GenericRecord> recordMap = new HashMap<>();
        while (recordIterator.hasNext()) {
            GenericRecord record = recordIterator.next();
            recordMap.put(record.get(DataCloudConstants.PROFILE_ATTR_ATTRNAME).toString(), record);
        }
        return recordMap;
    }

    private String getStatsVersion(String dataCloudVersion, ColumnSelection.Predefined predefined) {
        DataCloudVersion fullVersion = versionEntityMgr.findVersion(dataCloudVersion);
        String statsVersion = ColumnSelection.Predefined.Segment.equals(predefined)
                ? fullVersion.getSegmentStatsVersion() : fullVersion.getEnrichmentStatsVersion();
        if (StringUtils.isBlank(statsVersion)) {
            throw new IllegalStateException(
                    "There is not " + predefined + " stats version for data cloud version " + dataCloudVersion);
        }
        return statsVersion;
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

        statsCache = WatcherCache.builder() //
                .name("AMStatsCache") //
                .watch(AMRelease) //
                .maximum(10) //
                .load(dataCloudVersion -> readStatsPairFromHdfs((String) dataCloudVersion)) //
                .initKeys(new String[] { versionEntityMgr.currentApprovedVersionAsString() }) //
                .build();
    }

}
