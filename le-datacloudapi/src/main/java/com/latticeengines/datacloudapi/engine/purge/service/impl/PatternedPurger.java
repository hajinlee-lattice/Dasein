package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.service.HiveTableService;
import com.latticeengines.datacloudapi.engine.purge.service.SourcePurger;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;

/**
 * The entire source will be purged
 */
public abstract class PatternedPurger implements SourcePurger {

    @Autowired
    HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    HiveTableService hiveTableService;

    @Autowired
    HdfsSourceEntityMgr hdfsSourceEntityMgr;

    long DAY_IN_MS = 1000 * 60 * 60 * 24;

    public abstract boolean isToBak();

    public abstract String getSourcePrefix();

    public abstract int getRetainDays();

    @Override
    public List<PurgeSource> findSourcesToPurge(final boolean debug) {
        List<String> srcNames = findSourceNames(debug);
        return constructPurgeSources(srcNames);
    }

    private List<String> findSourceNames(final boolean debug) {
        String basePath = hdfsPathBuilder.constructSourceBaseDir().toString();

        HdfsUtils.HdfsFileFilter filter = new HdfsUtils.HdfsFileFilter() {
            @Override
            public boolean accept(FileStatus file) {
                if (file == null) {
                    return false;
                }
                String name = file.getPath().getName().toString();
                if (!name.startsWith(getSourcePrefix())) {
                    return false;
                }
                if (debug) {
                    return true;
                }
                long lastModifiedTime = file.getModificationTime();
                if (System.currentTimeMillis() - lastModifiedTime <= getRetainDays() * DAY_IN_MS) {
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

    private List<PurgeSource> constructPurgeSources(List<String> srcNames) {
        List<PurgeSource> toPurge = new ArrayList<>();
        srcNames.forEach(srcName -> {
            String hdfsPath = hdfsPathBuilder.constructSourceDir(srcName).toString();
            List<String> hdfsPaths = Collections.singletonList(hdfsPath);
            List<String> versions = hdfsSourceEntityMgr.getVersions(new GeneralSource(srcName));
            List<String> hiveTables = null;
            if (!CollectionUtils.isEmpty(versions)) {
                hiveTables = new ArrayList<>();
                for (String version : versions) {
                    hiveTables.add(hiveTableService.tableName(srcName, version));
                }
            }
            PurgeSource purgeSource = new PurgeSource(srcName, hdfsPaths, hiveTables, isToBak());
            toPurge.add(purgeSource);
        });
        return toPurge;
    }
}
