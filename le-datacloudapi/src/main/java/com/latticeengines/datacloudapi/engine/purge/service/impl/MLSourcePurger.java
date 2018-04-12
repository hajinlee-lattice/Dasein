package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

public abstract class MLSourcePurger extends ConfigurablePurger {

    private static Logger log = LoggerFactory.getLogger(MLSourcePurger.class);

    private static final String VERSION_FORMAT = "yyyy-MM-dd";

    protected abstract String getRootPath();

    @Override
    public SourceType getSourceType() {
        return SourceType.ML_SOURCE;
    }

    @Override
    protected List<String> findAllVersions(PurgeStrategy strategy) {
        try {
            List<FileStatus> fileStatus = HdfsUtils.getFileStatusesForDir(yarnConfiguration, getRootPath(), null);
            List<String> versions = new ArrayList<>();
            fileStatus.forEach(status -> {
                if (status.isDirectory() && isValidVersion(status.getPath().getName())) {
                    versions.add(status.getPath().getName());
                }
            });
            return versions;
        } catch (IOException e) {
            log.error("Fail to get file status for hdfs path " + getRootPath(), e);
            return null;
        }
    }

    @Override
    protected List<String> findVersionsToDelete(PurgeStrategy strategy, List<String> currentVersions,
            final boolean debug) {
        List<String> toDelete = new ArrayList<>();
        currentVersions.forEach(version -> {
            try {
                Date date = new SimpleDateFormat(VERSION_FORMAT).parse(version);
                Date now = new Date();
                int days = (int) ((now.getTime() - date.getTime()) / DAY_IN_MS);
                if (days > (strategy.getHdfsDays() + strategy.getS3Days() + strategy.getGlacierDays())) {
                    toDelete.add(version);
                }
            } catch (ParseException e) {
                log.error("Fail to parse version " + version + " for source " + strategy.getSource());
            }
        });
        return toDelete;
    }

    @Override
    protected List<String> findVersionsToBak(PurgeStrategy strategy, List<String> currentVersions,
            final boolean debug) {
        List<String> toBak = new ArrayList<>();
        currentVersions.forEach(version -> {
            try {
                Date date = new SimpleDateFormat(VERSION_FORMAT).parse(version);
                Date now = new Date();
                int days = (int) ((now.getTime() - date.getTime()) / DAY_IN_MS);
                if (days > strategy.getHdfsDays()
                        && days <= (strategy.getHdfsDays() + strategy.getS3Days() + strategy.getGlacierDays())) {
                    toBak.add(version);
                }
            } catch (ParseException e) {
                log.error("Fail to parse version " + version + " for source " + strategy.getSource());
            }
        });
        return toBak;
    }

    @Override
    protected Pair<List<String>, List<String>> constructHdfsPathsHiveTables(PurgeStrategy strategy,
            List<String> versions) {
        List<String> hdfsPaths = new ArrayList<>();
        List<String> hiveTables = new ArrayList<>();
        versions.forEach(version -> {
            String hdfsPath = new Path(getRootPath(), version).toString();
            hdfsPaths.add(hdfsPath);
        });

        return Pair.of(hdfsPaths, hiveTables);
    }

    private boolean isValidVersion(String version) {
        try {
            Date date = new SimpleDateFormat(VERSION_FORMAT).parse(version);
        } catch (ParseException e) {
            return false;
        }
        return true;
    }

}
