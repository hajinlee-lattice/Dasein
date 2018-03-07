package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
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

    @Autowired
    PurgeStrategyEntityMgr purgeStrategyEntityMgr;

    @Autowired
    HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    HiveTableService hiveTableService;

    public abstract boolean isToBak();

    public abstract SourceType getSourceType();

    public abstract List<String> findAllVersions(PurgeStrategy strategy);

    public abstract Pair<List<String>, List<String>> constructHdfsPathsHiveTables(PurgeStrategy strategy,
            List<String> versions);

    @Override
    public List<PurgeSource> findSourcesToPurge(boolean debug) {
        List<PurgeStrategy> strategies = purgeStrategyEntityMgr.findStrategiesByType(getSourceType());
        if (CollectionUtils.isEmpty(strategies)) {
            return Collections.<PurgeSource> emptyList();
        }
        List<PurgeSource> toPurge = new ArrayList<>();
        strategies.forEach(strategy -> {
            PurgeSource ent = constructPurgeSource(strategy);
            if (ent != null) {
                toPurge.add(ent);
            }
        });
        return toPurge;
    }

    private PurgeSource constructPurgeSource(PurgeStrategy strategy) {
        Pair<List<String>, List<String>> hdfsPathsHiveTables = getHdfsPathsHiveTablesToPurge(strategy);
        if (hdfsPathsHiveTables == null) {
            return null;
        }
        PurgeSource purgeSource = new PurgeSource(strategy.getSource(), hdfsPathsHiveTables.getLeft(),
                hdfsPathsHiveTables.getRight(), isToBak());
        purgeSource.setS3Days(strategy.getS3Days());
        purgeSource.setGlacierDays(strategy.getGlacierDays());
        return purgeSource;
    }

    private Pair<List<String>, List<String>> getHdfsPathsHiveTablesToPurge(PurgeStrategy strategy) {
        if (strategy.getHdfsVersions() == 0) {
            throw new RuntimeException(
                    "HDFS version for source " + strategy.getSource() + " is set as 0. Too dangerous");
        }
        List<String> versionsToPurge = findVersionsToPurge(strategy);
        if (CollectionUtils.isEmpty(versionsToPurge)) {
            return null;
        }
        return constructHdfsPathsHiveTables(strategy, versionsToPurge);
    }

    private List<String> findVersionsToPurge(PurgeStrategy strategy) {
        List<String> versionsToPurge = findAllVersions(strategy);
        if (CollectionUtils.isEmpty(versionsToPurge)) {
            return null;
        }
        Collections.sort(versionsToPurge);
        if (versionsToPurge.size() <= strategy.getHdfsVersions()) {
            return null;
        }
        for (int i = 0; i < strategy.getHdfsVersions(); i++) {
            versionsToPurge.remove(versionsToPurge.size() - 1); // Retain latest
                                                                // versions
        }
        return versionsToPurge;
    }
}
