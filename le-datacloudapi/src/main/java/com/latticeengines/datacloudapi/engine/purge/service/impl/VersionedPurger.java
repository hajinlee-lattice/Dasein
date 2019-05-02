package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.purge.entitymgr.PurgeStrategyEntityMgr;
import com.latticeengines.datacloud.etl.service.HiveTableService;
import com.latticeengines.datacloudapi.engine.purge.service.SourcePurger;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

/**
 * Source is purged version by version
 */
public abstract class VersionedPurger implements SourcePurger {

    private static Logger log = LoggerFactory.getLogger(VersionedPurger.class);

    @Autowired
    protected PurgeStrategyEntityMgr purgeStrategyEntityMgr;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected HiveTableService hiveTableService;

    @Autowired
    protected DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    @Autowired
    protected DataCloudVersionService dataCloudVersionService;

    @Autowired
    protected Configuration yarnConfiguration;

    protected long DAY_IN_MS = 1000 * 60 * 60 * 24;

    protected abstract SourceType getSourceType();

    /**
     * Override this method if the source needs to be dealt with specially
     */
    protected List<String> findVersionsToDelete(PurgeStrategy strategy, List<String> allVersions,
            final boolean debug) {
        if (!strategy.isNoBak()) {
            return null;
        }
        return findVersionsToPurge(strategy, allVersions, debug);
    }


    /**
     * Override this method if the source needs to be dealt with specially
     */
    protected List<String> findVersionsToBak(PurgeStrategy strategy, List<String> allVersions,
            final boolean debug) {
        if (strategy.isNoBak()) {
            return null;
        }
        return findVersionsToPurge(strategy, allVersions, debug);
    }

    /**
     * Override this method if the source needs to be dealt with specially
     */
    protected Pair<List<String>, List<String>> constructHdfsPathsHiveTables(PurgeStrategy strategy,
            List<String> versions) {
        List<String> hdfsPaths = new ArrayList<>();
        List<String> hiveTables = new ArrayList<>();
        versions.forEach(version -> {
            String hdfsPath = hdfsPathBuilder.constructSnapshotDir(strategy.getSource(), version).toString();
            String schemaPath = hdfsPathBuilder.constructSchemaDir(strategy.getSource(), version).toString();
            hdfsPaths.add(hdfsPath);
            hdfsPaths.add(schemaPath);
            String hiveTable = hiveTableService.tableName(strategy.getSource(), version);
            hiveTables.add(hiveTable);
        });

        return Pair.of(hdfsPaths, hiveTables);
    }

    /**
     * Override this method if the source needs to be dealt with specially
     */
    protected List<String> findAllVersions(PurgeStrategy strategy) {
        try {
            return hdfsSourceEntityMgr.getVersions(new GeneralSource(strategy.getSource()));
        } catch (Exception ex) {
            log.error("Fail to get versions for source " + strategy.getSource(), ex);
        }
        return null;
    }

    @Override
    public List<PurgeSource> findSourcesToPurge(final boolean debug) {
        List<PurgeStrategy> strategies = purgeStrategyEntityMgr.findStrategiesByType(getSourceType());
        if (CollectionUtils.isEmpty(strategies)) {
            return null;
        }
        List<PurgeSource> toPurge = new ArrayList<>();
        strategies.forEach(strategy -> {
            // check whether source exists or no : if not existing continue to
            // next loop iteration and skip constructPurgeSources
            Source source = null;
            if (strategy.getSourceType().equals(SourceType.INGESTION_SOURCE)) {
                source = new IngestionSource();
                ((IngestionSource) source).setIngestionName(strategy.getSource());
            } else {
                source = new GeneralSource(strategy.getSource());
            }
            SourceType arrOfSrcTypes[] = new SourceType[] { SourceType.HDFS_DIR,
                    SourceType.TEMP_SOURCE, SourceType.TIMESERIES_SOURCE };
            List<SourceType> listOfSrcTypes = Arrays.asList(arrOfSrcTypes);

            if ((!listOfSrcTypes.contains(strategy.getSourceType())
                    && hdfsSourceEntityMgr.checkSourceExist(source))
                    || (listOfSrcTypes.contains(strategy.getSourceType())
                            && isSourceExisted(strategy))) {
                List<PurgeSource> list = constructPurgeSource(strategy, debug);
                if (CollectionUtils.isNotEmpty(list)) {
                    toPurge.addAll(list);
                }
            }
        });
        return toPurge;
    }

    private List<PurgeSource> constructPurgeSource(PurgeStrategy strategy, final boolean debug) {
        List<String> currentVersions = findAllVersions(strategy);
        Pair<List<String>, List<String>> pathsToDelete = findPathsToDelete(strategy, currentVersions, debug);
        Pair<List<String>, List<String>> pathsToBak = findPathsToBak(strategy, currentVersions, debug);

        List<PurgeSource> list = new ArrayList<>();
        if (pathsToDelete != null) {
            PurgeSource purgeSource = new PurgeSource(strategy.getSource(), pathsToDelete.getLeft(),
                    pathsToDelete.getRight(), false);
            list.add(purgeSource);
        }
        if (pathsToBak != null) {
            PurgeSource purgeSource = new PurgeSource(strategy.getSource(), pathsToBak.getLeft(), pathsToBak.getRight(),
                    true);
            purgeSource.setGlacierDays(strategy.getGlacierDays());
            purgeSource.setS3Days(strategy.getS3Days());
            list.add(purgeSource);
        }
        return list;
    }

    private Pair<List<String>, List<String>> findPathsToDelete(PurgeStrategy strategy, List<String> allVersions,
            final boolean debug) {
        if (CollectionUtils.isEmpty(allVersions)) {
            return null;
        }
        if ((strategy.getHdfsVersions() == null && strategy.getHdfsDays() == null)
                || (strategy.getHdfsVersions() != null && strategy.getHdfsVersions() <= 0)
                || (strategy.getHdfsDays() != null && strategy.getHdfsDays() <= 0)) {
            throw new RuntimeException(
                    "HDFS versions/days for source " + strategy.getSource() + " must be greater than 0.");
        }
        List<String> versionsToDelete = findVersionsToDelete(strategy, allVersions, debug);
        if (CollectionUtils.isEmpty(versionsToDelete)) {
            return null;
        }
        return constructHdfsPathsHiveTables(strategy, versionsToDelete);
    }

    private Pair<List<String>, List<String>> findPathsToBak(PurgeStrategy strategy, List<String> allVersions,
            final boolean debug) {
        if (CollectionUtils.isEmpty(allVersions)) {
            return null;
        }
        if ((strategy.getHdfsVersions() == null && strategy.getHdfsDays() == null)
                || (strategy.getHdfsVersions() != null && strategy.getHdfsVersions() <= 0)
                || (strategy.getHdfsDays() != null && strategy.getHdfsDays() <= 0)) {
            throw new RuntimeException(
                    "HDFS versions/days for source " + strategy.getSource() + " must be greater than 0.");
        }
        List<String> versionsToBak = findVersionsToBak(strategy, allVersions, debug);
        if (CollectionUtils.isEmpty(versionsToBak)) {
            return null;
        }
        return constructHdfsPathsHiveTables(strategy, versionsToBak);
    }

    private List<String> findVersionsToPurge(PurgeStrategy strategy, List<String> allVersions,
            final boolean debug) {
        Collections.sort(allVersions);

        if (strategy.getHdfsVersions() != null) {
            if (allVersions.size() <= strategy.getHdfsVersions()) {
                return null;
            }
            for (int i = 0; i < strategy.getHdfsVersions(); i++) {
                allVersions.remove(allVersions.size() - 1);
            }
        }

        if (strategy.getHdfsDays() != null) {
            Set<String> versionSet = new HashSet<>(allVersions);
            String sourcePath = hdfsPathBuilder.constructSnapshotRootDir(strategy.getSource()).toString();
            try {
                List<FileStatus> versionStatus = HdfsUtils.getFileStatusesForDir(yarnConfiguration, sourcePath, null);
                versionStatus.forEach(status -> {
                    if (status.isDirectory() && versionSet.contains(status.getPath().getName())
                            && System.currentTimeMillis() - status.getModificationTime() <= strategy.getHdfsDays()
                                    * DAY_IN_MS) {
                        allVersions.remove(status.getPath().getName());
                    }
                });
            } catch (IOException e) {
                throw new RuntimeException("Fail to get file status for hdfs path " + sourcePath, e);
            }
        }

        return allVersions;
    }

    @Override
    public boolean isSourceExisted(PurgeStrategy strategy) {
        Source source = new GeneralSource(strategy.getSource());
        return hdfsSourceEntityMgr.checkSourceExist(source);
    }
}
