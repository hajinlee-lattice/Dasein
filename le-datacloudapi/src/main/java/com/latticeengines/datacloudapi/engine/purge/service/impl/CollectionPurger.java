package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
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
 * Purge sources/directories by checking last modified date
 */
public abstract class CollectionPurger implements SourcePurger {

    @Autowired
    private PurgeStrategyEntityMgr purgeStrategyEntityMgr;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected HiveTableService hiveTableService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    protected long DAY_IN_MS = 1000 * 60 * 60 * 24;

    protected abstract SourceType getSourceType();

    // SourceName -> HdfsPath
    protected abstract Map<String, String> findSourcePaths(PurgeStrategy strategy, final boolean debug);

    protected abstract List<PurgeSource> constructPurgeSources(PurgeStrategy strategy, Map<String, String> sourcePaths);

    @Override
    public List<PurgeSource> findSourcesToPurge(final boolean debug) {
        List<PurgeStrategy> strategies = purgeStrategyEntityMgr.findStrategiesByType(getSourceType());
        List<PurgeSource> list = new ArrayList<>();
        for (PurgeStrategy strategy : strategies) {
            Map<String, String> sourcePaths = findSourcePaths(strategy, debug);
            list.addAll(constructPurgeSources(strategy, sourcePaths));
        }
        return list;
    }


}
