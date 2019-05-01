package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
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
 * Purge sources/directories by checking last modified date
 */
public abstract class CollectionPurger implements SourcePurger {

    private static final Logger log = LoggerFactory.getLogger(CollectionPurger.class);

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

    // SourceName -> HdfsPath
    protected abstract Map<String, String> findSourcePaths(PurgeStrategy strategy, boolean debug);

    protected abstract List<PurgeSource> constructPurgeSources(PurgeStrategy strategy, Map<String, String> sourcePaths);

    @Override
    public List<PurgeSource> findSourcesToPurge(final boolean debug) {
        List<PurgeStrategy> strategies = purgeStrategyEntityMgr.findStrategiesByType(getSourceType());
        List<PurgeSource> list = new ArrayList<>();
        for (PurgeStrategy strategy : strategies) {
            // check whether source exists or no : if not existing continue to
            // next loop iteration and skip constructPurgeSources
            Source source = null;
            if (strategy.getSourceType().equals(SourceType.INGESTION_SOURCE)) {
                source = new IngestionSource();
                ((IngestionSource) source).setIngestionName(strategy.getSource());
            } else {
                source = new GeneralSource(strategy.getSource());
            }
            try {
                if ((!strategy.getSourceType().equals(SourceType.HDFS_DIR)
                        && hdfsSourceEntityMgr.checkSourceExist(source))
                        || (strategy.getSourceType().equals(SourceType.HDFS_DIR)
                                && HdfsUtils.isDirectory(yarnConfiguration,
                                        strategy.getHdfsBasePath()))) {
                    Map<String, String> sourcePaths = findSourcePaths(strategy, debug);
                    list.addAll(constructPurgeSources(strategy, sourcePaths));
                }
            } catch (IOException e) {
                log.info("Exception in checking source : " + strategy.getSource() + " directory : "
                        + e);
            }
        }
        return list;
    }


}
