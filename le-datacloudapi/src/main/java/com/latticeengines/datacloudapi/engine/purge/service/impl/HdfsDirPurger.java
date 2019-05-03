package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

@Component("hdfsDirPurger")
public class HdfsDirPurger extends CollectionPurger {

    public static final String HIVE = "Hive";
    private static final Logger log = LoggerFactory.getLogger(HdfsDirPurger.class);

    @Override
    protected SourceType getSourceType() {
        return SourceType.HDFS_DIR;
    }

    @Override
    protected Map<String, String> findSourcePaths(PurgeStrategy strategy, boolean debug) {
        if (StringUtils.isBlank(strategy.getHdfsBasePath())) {
            throw new RuntimeException(
                    "HDFS_DIR type must be provided with HdfsBasePath. Please check source " + strategy.getSource());
        }
        if (strategy.getHdfsDays() == null || strategy.getHdfsDays() <= 0) {
            throw new RuntimeException(
                    "HDFS versions/days for source " + strategy.getSource() + " must be greater than 0.");
        }
        HdfsUtils.HdfsFileFilter filter = new HdfsUtils.HdfsFileFilter() {
            @Override
            public boolean accept(FileStatus file) {
                if (file == null) {
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
            files = HdfsUtils.getFileStatusesForDir(yarnConfiguration, strategy.getHdfsBasePath(), filter);
        } catch (IOException e) {
            throw new RuntimeException("Fail to get sub-items in path " + strategy.getHdfsBasePath(), e);
        }
        Map<String, String> sourcePaths = new HashMap<>();
        files.forEach(file -> {
            String fullPath = file.getPath().toString();
            sourcePaths.put(file.getPath().getName(), fullPath.substring(fullPath.indexOf(strategy.getHdfsBasePath())));
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
            if (HIVE.equals(strategy.getSource())) {
                hiveTables = Collections.singletonList(srcName.toLowerCase());
            }
            PurgeSource purgeSource = new PurgeSource(srcName, hdfsPaths, hiveTables, !strategy.isNoBak());
            if (!strategy.isNoBak()) {
                purgeSource.setS3Days(strategy.getS3Days());
                purgeSource.setGlacierDays(strategy.getGlacierDays());
            }
            toPurge.add(purgeSource);
        }
        return toPurge;
    }

    @Override
    public boolean isSourceExisted(PurgeStrategy strategy) {
        try {
            return HdfsUtils.isDirectory(yarnConfiguration, strategy.getHdfsBasePath());
        } catch (IOException e) {
            log.info("Exception in checking source path : " + e);
        }
        return false;
    }

}
