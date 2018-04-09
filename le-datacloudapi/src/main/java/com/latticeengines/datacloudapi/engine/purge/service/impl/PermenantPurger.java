package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.purge.entitymgr.PurgeStrategyEntityMgr;
import com.latticeengines.datacloud.etl.service.HiveTableService;
import com.latticeengines.datacloudapi.engine.purge.service.SourcePurger;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;

/**
 * The entire source will be purged
 */
@Component("permenantPurger")
public class PermenantPurger implements SourcePurger {

    private static Logger log = LoggerFactory.getLogger(PermenantPurger.class);

    @Autowired
    private PurgeStrategyEntityMgr purgeStrategyEntityMgr;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private HiveTableService hiveTableService;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    private long DAY_IN_MS = 1000 * 60 * 60 * 24;

    @Override
    public List<PurgeSource> findSourcesToPurge(final boolean debug) {
        List<PurgeStrategy> strategies = purgeStrategyEntityMgr
                .findStrategiesByType(PurgeStrategy.SourceType.TEMP_SOURCE);
        List<PurgeSource> list = new ArrayList<>();
        for (PurgeStrategy strategy : strategies) {
            List<String> srcNames = findSourceNames(strategy, debug);
            list.addAll(constructPurgeSources(strategy, srcNames));
        }
        return list;
    }

    private List<String> findSourceNames(PurgeStrategy strategy, final boolean debug) {
        String basePath = hdfsPathBuilder.constructSourceBaseDir().toString();

        HdfsUtils.HdfsFileFilter filter = new HdfsUtils.HdfsFileFilter() {
            @Override
            public boolean accept(FileStatus file) {
                if (file == null) {
                    return false;
                }
                String name = file.getPath().getName().toString();
                if (!name.startsWith(strategy.getSource())) {
                    return false;
                }
                if (debug) {
                    return true;
                }
                long lastModifiedTime = file.getModificationTime();
                if (System.currentTimeMillis() - lastModifiedTime <= strategy.getHdfsDays() * DAY_IN_MS) {
                    return false;
                }
                return true;
            }

        };

        List<FileStatus> files = new ArrayList<>();
        try {
            files = HdfsUtils.getFileStatusesForDir(yarnConfiguration, basePath, filter);
        } catch (IOException e) {
            throw new RuntimeException("Fail to get source names in path " + basePath, e);
        }
        List<String> sourceNames = new ArrayList<>();
        files.forEach(file -> {
            sourceNames.add(file.getPath().getName());
        });

        return sourceNames;
    }

    private List<PurgeSource> constructPurgeSources(PurgeStrategy strategy, List<String> srcNames) {
        List<PurgeSource> toPurge = new ArrayList<>();
        srcNames.forEach(srcName -> {
            String hdfsPath = hdfsPathBuilder.constructSourceDir(srcName).toString();
            List<String> hdfsPaths = Collections.singletonList(hdfsPath);
            List<String> hiveTables = null;
            if (!srcName.startsWith(DataCloudConstants.PIPELINE_TEMPSRC_PREFIX)) {
                try {
                    List<String> versions = hdfsSourceEntityMgr.getVersions(new GeneralSource(srcName));
                    if (!CollectionUtils.isEmpty(versions)) {
                        hiveTables = new ArrayList<>();
                        for (String version : versions) {
                            hiveTables.add(hiveTableService.tableName(srcName, version));
                        }
                    }
                } catch (Exception ex) {
                    log.warn("Fail to find versions for source " + srcName, ex);
                }
            }
            PurgeSource purgeSource = new PurgeSource(srcName, hdfsPaths, hiveTables, false);
            toPurge.add(purgeSource);
        });
        return toPurge;
    }
}
