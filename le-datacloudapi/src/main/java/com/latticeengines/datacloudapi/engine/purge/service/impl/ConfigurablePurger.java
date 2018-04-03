package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
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
public abstract class ConfigurablePurger implements SourcePurger {

    private static Logger log = LoggerFactory.getLogger(ConfigurablePurger.class);

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

    public abstract SourceType getSourceType();

    /**
     * Override this method if the source needs to be dealt with specially
     */
    protected List<String> findVersionsToDelete(PurgeStrategy strategy, List<String> currentVersions,
            final boolean debug) {
        return null;
    }


    /**
     * Override this method if the source needs to be dealt with specially
     */
    protected List<String> findVersionsToBak(PurgeStrategy strategy, List<String> currentVersions,
            final boolean debug) {
        Collections.sort(currentVersions);
        if (currentVersions.size() <= strategy.getHdfsVersions()) {
            return null;
        }
        for (int i = 0; i < strategy.getHdfsVersions(); i++) {
            currentVersions.remove(currentVersions.size() - 1); // Retain latest versions
        }
        return currentVersions;
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

    @Override
    public List<PurgeSource> findSourcesToPurge(final boolean debug) {
        List<PurgeStrategy> strategies = purgeStrategyEntityMgr.findStrategiesByType(getSourceType());
        if (CollectionUtils.isEmpty(strategies)) {
            return Collections.<PurgeSource> emptyList();
        }
        List<PurgeSource> toPurge = new ArrayList<>();
        strategies.forEach(strategy -> {
            List<PurgeSource> list = constructPurgeSource(strategy, debug);
            if (CollectionUtils.isNotEmpty(list)) {
                toPurge.addAll(list);
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

    protected List<String> findAllVersions(PurgeStrategy strategy) {
        try {
            return hdfsSourceEntityMgr.getVersions(new GeneralSource(strategy.getSource()));
        } catch (Exception ex) {
            log.error("Fail to get versions for source " + strategy.getSource(), ex);
        }
        return null;
    }

    private Pair<List<String>, List<String>> findPathsToDelete(PurgeStrategy strategy, List<String> currentVersions,
            final boolean debug) {
        if (CollectionUtils.isEmpty(currentVersions)) {
            return null;
        }
        if (strategy.getHdfsVersions() <= 0) {
            throw new RuntimeException(
                    "HDFS version for source " + strategy.getSource() + " is set as 0 or invalid.");
        }
        List<String> versionsToDelete = findVersionsToDelete(strategy, currentVersions, debug);
        if (CollectionUtils.isEmpty(versionsToDelete)) {
            return null;
        }
        return constructHdfsPathsHiveTables(strategy, versionsToDelete);
    }

    private Pair<List<String>, List<String>> findPathsToBak(PurgeStrategy strategy, List<String> currentVersions,
            final boolean debug) {
        if (CollectionUtils.isEmpty(currentVersions)) {
            return null;
        }
        if (strategy.getHdfsVersions() == 0) {
            throw new RuntimeException(
                    "HDFS version for source " + strategy.getSource() + " is set as 0. Too dangerous");
        }
        List<String> versionsToBak = findVersionsToBak(strategy, currentVersions, debug);
        if (CollectionUtils.isEmpty(versionsToBak)) {
            return null;
        }
        return constructHdfsPathsHiveTables(strategy, versionsToBak);
    }
}
