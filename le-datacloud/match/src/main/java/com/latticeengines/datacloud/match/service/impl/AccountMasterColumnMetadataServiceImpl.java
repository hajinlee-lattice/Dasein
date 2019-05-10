package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.camille.watchers.CamilleWatcher.AMRelease;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.camille.exposed.watchers.WatcherCache;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.match.exposed.service.MetadataColumnService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.util.StatsCubeUtils;

@Component("accountMasterColumnMetadataService")
public class AccountMasterColumnMetadataServiceImpl extends BaseColumnMetadataServiceImpl<AccountMasterColumn> {

    private WatcherCache<String, Pair<StatsCube, TopNTree>> statsCache;

    @Resource(name = "accountMasterColumnService")
    private MetadataColumnService<AccountMasterColumn> accountMasterColumnService;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Inject
    private S3Service s3Service;

    @Value("${datacloud.collection.s3bucket}")
    private String s3Bucket;

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
        return getStatsCache().get(dataCloudVersion).getLeft();
    }

    @Override
    public TopNTree getTopNTree(String dataCloudVersion) {
        return getStatsCache().get(dataCloudVersion).getRight();
    }

    @SuppressWarnings("unchecked")
    private WatcherCache<String, Pair<StatsCube, TopNTree>> getStatsCache() {
        if (statsCache == null) {
            synchronized (this) {
                if (statsCache == null) {
                    statsCache = WatcherCache.builder() //
                            .name("AMStatsCache") //
                            .watch(AMRelease.name()) //
                            .maximum(10) //
                            .load(dataCloudVersion -> readStatsPairFromHdfs((String) dataCloudVersion)) //
                            .initKeys(new String[] { versionEntityMgr.currentApprovedVersionAsString() }) //
                            .build();
                }
            }
        }
        return statsCache;
    }

    private Pair<StatsCube, TopNTree> readStatsPairFromHdfs(String dataCloudVersion) {
        StatsCube statsCube = readStatisticsFromHdfs(dataCloudVersion);
        List<ColumnMetadata> cms = fromPredefinedSelection(Predefined.Enrichment, dataCloudVersion);
        // AccountMasterStats has nothing to do with EntityMatch feature flag
        TopNTree topNTree = StatsCubeUtils.constructTopNTree( //
                ImmutableMap.of("LatticeAccount", statsCube), //
                ImmutableMap.of("LatticeAccount", cms), //
                true, ColumnSelection.Predefined.Enrichment, false);
        return ImmutablePair.of(statsCube, topNTree);
    }

    private StatsCube readStatisticsFromHdfs(String dataCloudVersion) {
        DataCloudVersion fullVersion = versionEntityMgr.findVersion(dataCloudVersion);
        String statsVersion = fullVersion.getEnrichmentStatsVersion();
        if (StringUtils.isBlank(statsVersion)) {
            throw new IllegalStateException(
                    "There is not enrichment stats version for data cloud version " + dataCloudVersion);
        }
        Iterator<GenericRecord> recordIterator = getEnrichStatsRecords(statsVersion);
        return StatsCubeUtils.parseAvro(recordIterator);
    }

    private Iterator<GenericRecord> getEnrichStatsRecords(String statsVersion) {
        String snapshotDir = getEnrichStatsSnapshotDir(statsVersion);
        List<S3ObjectSummary> summaries = s3Service.listObjects(s3Bucket, snapshotDir);
        List<GenericRecord> records = new ArrayList<>();
        summaries.forEach(summary -> {
            String key = summary.getKey();
            if (key.endsWith(".avro")) {
                RetryTemplate retry = RetryUtils.getRetryTemplate(5);
                try {
                    List<GenericRecord> recordsInAvro = retry.execute(context -> {
                        try (InputStream is = s3Service.readObjectAsStream(s3Bucket, key)) {
                            return AvroUtils.readFromInputStream(is);
                        }
                    });
                    records.addAll(recordsInAvro);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read avro " + key, e);
                }
            }
        });
        return records.iterator();
    }

    private String getEnrichStatsSnapshotDir(String statsVersion) {
        String sourceName = "AccountMasterEnrichmentStats";
        // for now use the same path as hdfs
        // but remove the first /
        String path = hdfsPathBuilder.constructSnapshotDir(sourceName, statsVersion).toString();
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }

}
