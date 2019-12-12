package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.purge.entitymgr.PurgeStrategyEntityMgr;
import com.latticeengines.datacloud.etl.service.HiveTableService;
import com.latticeengines.datacloudapi.engine.purge.service.SourcePurger;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;


/**
 * For sources without version, or for source whose version we don't care (When
 * we purge the source, we always want to purge the entire source). The
 * retention policy is configured by how long they have existed on HDFS.
 */
public abstract class CollectionPurger implements SourcePurger {

    @Inject
    private PurgeStrategyEntityMgr purgeStrategyEntityMgr;

    @Inject
    protected HdfsPathBuilder hdfsPathBuilder;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    protected HiveTableService hiveTableService;

    @Inject
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    protected long DAY_IN_MS = 1000 * 60 * 60 * 24;

    protected abstract SourceType getSourceType();

    // SourceName -> HdfsPaths
    protected abstract Map<String, List<String>> findSourcePaths(PurgeStrategy strategy, boolean debug);

    @Override
    public List<PurgeSource> findSourcesToPurge(final boolean debug) {
        List<PurgeStrategy> strategies = purgeStrategyEntityMgr.findStrategiesByType(getSourceType());
        List<PurgeSource> list = new ArrayList<>();
        for (PurgeStrategy strategy : strategies) {

            // check whether source exists or no : if not existing continue to
            // next loop iteration and skip constructPurgeSources
            if (isSourceExisted(strategy)) {
                Map<String, List<String>> sourcePaths = findSourcePaths(strategy, debug);
                list.addAll(constructPurgeSources(strategy, sourcePaths));
            }
        }
        return list;
    }

    private List<PurgeSource> constructPurgeSources(PurgeStrategy strategy, Map<String, List<String>> sourcePaths) {
        List<PurgeSource> toPurge = new ArrayList<>();
        for (Map.Entry<String, List<String>> srcPath : sourcePaths.entrySet()) {
            String srcName = srcPath.getKey();
            List<String> hdfsPaths = srcPath.getValue();
            PurgeSource purgeSource = new PurgeSource(srcName, hdfsPaths);
            toPurge.add(purgeSource);
        }
        return toPurge;
    }

}
