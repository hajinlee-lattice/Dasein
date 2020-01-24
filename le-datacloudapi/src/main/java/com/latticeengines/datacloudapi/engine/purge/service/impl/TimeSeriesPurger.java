package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

/**
 * Targeting source:
 *
 * Currently not actively used. Previous use cases were MadisonLogic source
 * (deprecated), MatchHistory and ScoreHistory (migrated to S3).
 *
 * When to purge a version:
 *
 * TimeSeries source has version with timestamp format defined in VersionFormat.
 * Parse the timestamp of a version, if it's older than
 * {@link #PurgeStrategy.getHdfsDays}} number of days, purge it.
 */
@Component("timeSeriesPurger")
public class TimeSeriesPurger extends VersionedPurger {

    private static final Logger log = LoggerFactory.getLogger(TimeSeriesPurger.class);

    @Override
    protected SourceType getSourceType() {
        return SourceType.TIMESERIES_SOURCE;
    }

    @Override
    protected List<String> findAllVersions(PurgeStrategy strategy) {
        if (StringUtils.isBlank(strategy.getHdfsBasePath()) || StringUtils.isBlank(strategy.getVersionFormat())) {
            throw new IllegalArgumentException(
                    "TIMESERIES_SOURCE type must be provided with HdfsBasePath and VersionFormat, or change the type to GENERAL_SOURCE. Please check source "
                            + strategy.getSource());
        }
        try {
            List<FileStatus> fileStatus = HdfsUtils.getFileStatusesForDir(yarnConfiguration, strategy.getHdfsBasePath(),
                    null);
            List<String> versions = new ArrayList<>();
            fileStatus.forEach(status -> {
                if (status.isDirectory() && isValidVersion(status.getPath().getName(), strategy.getVersionFormat())) {
                    versions.add(status.getPath().getName());
                }
            });
            return versions;
        } catch (IOException e) {
            log.error("Fail to get file status for hdfs path " + strategy.getHdfsBasePath(), e);
            return null;
        }
    }

    @Override
    protected List<String> findPurgeVersions(PurgeStrategy strategy, List<String> allVersions,
            final boolean debug) {
        List<String> purgeVersions = new ArrayList<>();
        allVersions.forEach(version -> {
            try {
                Date date = new SimpleDateFormat(strategy.getVersionFormat()).parse(version);
                Date now = new Date();
                int days = (int) ((now.getTime() - date.getTime()) / DAY_IN_MS);
                if (days > strategy.getHdfsDays()) {
                    purgeVersions.add(version);
                }
            } catch (ParseException e) {
                log.error("Fail to parse version " + version + " for source " + strategy.getSource());
            }
        });
        return purgeVersions;
    }

    @Override
    protected List<String> constructHdfsPaths(PurgeStrategy strategy,
            List<String> versions) {
        List<String> hdfsPaths = new ArrayList<>();
        versions.forEach(version -> {
            String hdfsPath = new Path(strategy.getHdfsBasePath(), version).toString();
            hdfsPaths.add(hdfsPath);
        });

        return hdfsPaths;
    }

    private boolean isValidVersion(String version, String versionFormat) {
        try {
            new SimpleDateFormat(versionFormat).parse(version);
        } catch (ParseException e) {
            return false;
        }
        return true;
    }

    @Override
    public boolean isSourceExisted(PurgeStrategy strategy) {
        try {
            return HdfsUtils.isDirectory(yarnConfiguration, strategy.getHdfsBasePath());
        } catch (IOException e) {
            log.info("Exception in checking source directory path : " + strategy.getHdfsBasePath());
        }
        return false;
    }

}
