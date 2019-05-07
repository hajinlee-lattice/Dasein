package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

@Component("tempSourcePurger")
public class TempSourcePurger extends CollectionPurger {

    private static Logger log = LoggerFactory.getLogger(TempSourcePurger.class);

    @Override
    protected SourceType getSourceType() {
        return SourceType.TEMP_SOURCE;
    }

    // SourceName -> HdfsPath
    protected Map<String, String> findSourcePaths(PurgeStrategy strategy, final boolean debug) {
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
        Map<String, String> sourcePaths = new HashMap<>();
        files.forEach(file -> {
            String fullPath = file.getPath().toString();
            sourcePaths.put(file.getPath().getName(), fullPath.toString().substring(fullPath.indexOf(basePath)));
        });

        return sourcePaths;
    }

    @Override
    protected List<PurgeSource> constructPurgeSources(PurgeStrategy strategy, Map<String, String> sourcePaths) {
        List<PurgeSource> toPurge = new ArrayList<>();

        for (Map.Entry<String, String> srcPath : sourcePaths.entrySet()) {
            String srcName = srcPath.getKey();
            String hdfsPath = srcPath.getValue();
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
        }
        return toPurge;
    }

    @Override
    public boolean isSourceExisted(PurgeStrategy strategy) {
        return true; // for temp source no need to check path for finding sourceToPurge
    }

}
