package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.datacloud.etl.purge.entitymgr.PurgeStrategyEntityMgr;
import com.latticeengines.datacloudapi.engine.purge.service.SourcePurger;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

public abstract class ConfigurablePurger implements SourcePurger {

    @Autowired
    PurgeStrategyEntityMgr purgeStrategyEntityMgr;

    public abstract boolean isToBak();

    public abstract SourceType getSourceType();

    public abstract Pair<List<String>, List<String>> getHdfsPathsHiveTablesToPurge(PurgeStrategy strategy);

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
}
