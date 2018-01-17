package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.AMRelease;

import java.util.Iterator;
import java.util.List;

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
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;

@Component("accountMasterColumnMetadataService")
public class AccountMasterColumnMetadataServiceImpl extends BaseColumnMetadataServiceImpl<AccountMasterColumn> {

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
    public StatsCube getStatsCube(String dataCloudVersion) {
        return statsCache.get(dataCloudVersion).getLeft();
    }

    @Override
    public TopNTree getTopNTree(String dataCloudVersion) {
        return statsCache.get(dataCloudVersion).getRight();
    }

    private Pair<StatsCube, TopNTree> readStatsPairFromHdfs(String dataCloudVersion) {
        Statistics statistics = readStatisticsFromHdfs(dataCloudVersion);
        List<ColumnMetadata> cms = fromPredefinedSelection(Predefined.Enrichment, dataCloudVersion);
        StatsCube statsCube = StatsCubeUtils.toStatsCube(statistics, cms);
        TopNTree topNTree = StatsCubeUtils.toTopNTree(statistics, false, cms);
        return ImmutablePair.of(statsCube, topNTree);
    }

    private Statistics readStatisticsFromHdfs(String dataCloudVersion) {
        DataCloudVersion fullVersion = versionEntityMgr.findVersion(dataCloudVersion);
        String statsVersion = fullVersion.getEnrichmentStatsVersion();
        if (StringUtils.isBlank(statsVersion)) {
            throw new IllegalStateException(
                    "There is not enrichment stats version for data cloud version " + dataCloudVersion);
        }
        String sourceName = "AccountMasterEnrichmentStats";
        String snapshotDir = hdfsPathBuilder.constructSnapshotDir(sourceName, statsVersion).toString();
        Iterator<GenericRecord> recordIterator = AvroUtils.iterator(yarnConfiguration, snapshotDir + "/*.avro");
        StatsCube cube = StatsCubeUtils.parseAvro(recordIterator);
        List<ColumnMetadata> amAttrs = fromPredefinedSelection(ColumnSelection.Predefined.Enrichment, fullVersion.getVersion());
        BusinessEntity entity = BusinessEntity.LatticeAccount;
        return StatsCubeUtils.constructStatistics(ImmutableMap.of(entity, cube), ImmutableMap.of(entity, amAttrs));
    }

    @SuppressWarnings("unchecked")
    private void initCache() {
        statsCache = WatcherCache.builder() //
                .name("AMStatsCache") //
                .watch(AMRelease.name()) //
                .maximum(10) //
                .load(dataCloudVersion -> readStatsPairFromHdfs((String) dataCloudVersion)) //
                .initKeys(new String[] { versionEntityMgr.currentApprovedVersionAsString() }) //
                .build();
    }

}
