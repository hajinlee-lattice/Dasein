package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

/**
 * Targeting source:
 *
 * For HDFS path with sub-directories which should be purged by checking
 * sub-directory's created date is out of date.
 *
 * When to purge a sub-directory:
 *
 * If a sub-directory's created date is older than
 * {@link #PurgeStrategy.getHdfsDays}} number of days, purge the sub-directory.
 * {@link #PurgeStrategy.getHdfsVersions}} is not honored. Root directory is
 * never purged.
 */
@Component("hdfsDirPurger")
public class HdfsDirPurger extends CollectionPurger {

    private static final Logger log = LoggerFactory.getLogger(HdfsDirPurger.class);

    @Override
    protected SourceType getSourceType() {
        return SourceType.HDFS_DIR;
    }

    @Override
    protected Map<String, List<String>> findSourcePaths(PurgeStrategy strategy, boolean debug) {
        if (StringUtils.isBlank(strategy.getHdfsBasePath())) {
            throw new IllegalArgumentException(
                    "HDFS_DIR type must be provided with HdfsBasePath. Please check source " + strategy.getSource());
        }
        if (strategy.getHdfsDays() == null || strategy.getHdfsDays() <= 0) {
            throw new IllegalArgumentException(
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
        Map<String, List<String>> sourcePaths = new HashMap<>();
        files.forEach(file -> {
            String fullPath = file.getPath().toString();
            sourcePaths.putIfAbsent(strategy.getSource(), new ArrayList<>());
            sourcePaths.get(strategy.getSource()).add(fullPath.substring(fullPath.indexOf(strategy.getHdfsBasePath())));
        });

        return sourcePaths;
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
