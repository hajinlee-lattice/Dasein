package com.latticeengines.propdata.core.entitymgr.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.IngestedRawSource;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.util.TableUtils;

@Component("hdfsSourceEntityMgr")
public class HdfsSourceEntityMgrImpl implements HdfsSourceEntityMgr {

    private static final long SLEEP_DURATION_IN_EXCEPTION_HADLING = 1000L;

    private static final String SUCCESS_FILE_SUFFIX = "_SUCCESS";

    private static final String AVRO_FILE_EXTENSION = ".avro";

    private static final String WILD_CARD = "*";

    private static final String HDFS_PATH_SEPARATOR = "/";

    private static final Log log = LogFactory.getLog(HdfsSourceEntityMgrImpl.class);

    @Autowired
    HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    YarnConfiguration yarnConfiguration;

    @Override
    public String getCurrentVersion(Source source) {
        String versionFile = hdfsPathBuilder.constructVersionFile(source).toString();
        int retries = 0;
        while (retries++ < 3) {
            try {
                return HdfsUtils.getHdfsFileContents(yarnConfiguration, versionFile);
            } catch (Exception e) {
                sleep(SLEEP_DURATION_IN_EXCEPTION_HADLING);
            }
        }
        throw new RuntimeException("Could not determine the current version of source " + source.getSourceName());
    }

    @Override
    public synchronized void setCurrentVersion(Source source, String version) {
        String versionFile = hdfsPathBuilder.constructVersionFile(source).toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, versionFile)) {
                HdfsUtils.rmdir(yarnConfiguration, versionFile);
            }
            HdfsUtils.writeToFile(yarnConfiguration, versionFile, version);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Date getLatestTimestamp(IngestedRawSource source) {
        String versionFile = hdfsPathBuilder.constructLatestFile(source).toString();
        int retries = 0;
        while (retries++ < 3) {
            try {
                Long mills = Long.valueOf(HdfsUtils.getHdfsFileContents(yarnConfiguration, versionFile));
                return new Date(mills);
            } catch (Exception e) {
                sleep(SLEEP_DURATION_IN_EXCEPTION_HADLING);
            }
        }
        return null;
    }

    @Override
    public synchronized void setLatestTimestamp(IngestedRawSource source, Date timestamp) {
        Date currentTimestamp = getLatestTimestamp(source);
        if (currentTimestamp != null && currentTimestamp.after(timestamp)) {
            return;
        }

        String timestampFile = hdfsPathBuilder.constructLatestFile(source).toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, timestampFile)) {
                HdfsUtils.rmdir(yarnConfiguration, timestampFile);
            }
            HdfsUtils.writeToFile(yarnConfiguration, timestampFile, String.valueOf(timestamp.getTime()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void purgeSourceAtVersion(Source source, String version) {
        if (source instanceof IngestedRawSource) {
            throw new UnsupportedOperationException("Never purge collected source.");
        }

        String currentVersion = getCurrentVersion(source);
        if (currentVersion.equalsIgnoreCase(version)) {
            throw new RuntimeException(
                    "Cannot purge current version " + version + " for source" + source.getSourceName());
        }

        String path = hdfsPathBuilder.constructSnapshotDir(source, version).toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, path)) {
                HdfsUtils.rmdir(yarnConfiguration, path);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to purge " + source.getSourceName() + " at version " + version, e);
        }

        log.info("Purged " + source.getSourceName() + " at version " + version);
    }

    @Override
    public Table getTableAtVersion(Source source, String version) {
        if (source instanceof CollectedSource) {
            throw new UnsupportedOperationException(
                    "Do not know how to extract versioned table for " + CollectedSource.class);
        }
        if (source instanceof HasSqlPresence) {
            String path = hdfsPathBuilder.constructSnapshotDir(source, version).toString();
            return TableUtils.createTable(((HasSqlPresence) source).getSqlTableName(),
                    path + HDFS_PATH_SEPARATOR + WILD_CARD + AVRO_FILE_EXTENSION);
        } else {
            String path = hdfsPathBuilder.constructSnapshotDir(source, version).toString();
            return TableUtils.createTable(source.getSourceName(),
                    path + HDFS_PATH_SEPARATOR + WILD_CARD + AVRO_FILE_EXTENSION);
        }
    }

    @Override
    public List<String> getVersions(Source source) {
        String snapshot = hdfsPathBuilder.constructSnapshotRootDir(source).toString();
        List<String> versions = new ArrayList<>();
        try {
            for (String dir : HdfsUtils.getFilesForDir(yarnConfiguration, snapshot)) {
                if (HdfsUtils.isDirectory(yarnConfiguration, dir)) {
                    String version = dir.substring(dir.lastIndexOf(HDFS_PATH_SEPARATOR) + 1);
                    versions.add(version);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to get all versions for " + source.getSourceName());
        }
        return versions;
    }

    @Override
    public Table getCollectedTableSince(IngestedRawSource source, Date earliest) {
        String firstVersion = HdfsPathBuilder.dateFormat.format(earliest);

        String rawDir = hdfsPathBuilder.constructRawDir(source).toString();
        List<String> avroPaths = new ArrayList<>();
        try {
            for (String dir : HdfsUtils.getFilesForDir(yarnConfiguration, rawDir)) {
                if (HdfsUtils.isDirectory(yarnConfiguration, dir)) {
                    String version = dir.substring(dir.lastIndexOf(HDFS_PATH_SEPARATOR) + 1);
                    String success = rawDir + HDFS_PATH_SEPARATOR + version + HDFS_PATH_SEPARATOR + SUCCESS_FILE_SUFFIX;
                    if (version.compareTo(firstVersion) > 0 && HdfsUtils.fileExists(yarnConfiguration, success)) {
                        avroPaths.add(rawDir + HDFS_PATH_SEPARATOR + version + HDFS_PATH_SEPARATOR + WILD_CARD
                                + AVRO_FILE_EXTENSION);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to get all incremental raw data dirs for " + source.getSourceName());
        }
        return TableUtils.createTable(source.getSourceName(), avroPaths.toArray(new String[avroPaths.size()]),
                source.getPrimaryKey());
    }

    @Override
    public Long count(Source source, String version) {
        try {
            String avroDir;
            if (source instanceof CollectedSource) {
                String rawDir = hdfsPathBuilder.constructRawDir(source).toString();
                avroDir = rawDir + HDFS_PATH_SEPARATOR + version;
            } else {
                avroDir = hdfsPathBuilder.constructSnapshotDir(source, version).toString();
            }
            if (HdfsUtils.isDirectory(yarnConfiguration, avroDir)) {
                String success = avroDir + HDFS_PATH_SEPARATOR + SUCCESS_FILE_SUFFIX;
                if (HdfsUtils.fileExists(yarnConfiguration, success)) {
                    return AvroUtils.count(yarnConfiguration,
                            avroDir + HDFS_PATH_SEPARATOR + WILD_CARD + AVRO_FILE_EXTENSION);
                } else {
                    throw new RuntimeException(
                            "Cannot find _SUCCESS file in the avro dir, may be it is still being populated.");
                }
            } else {
                throw new RuntimeException("Cannot find avro dir " + avroDir);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to count source " + source.getSourceName() + " at version " + version,
                    e);
        }
    }

    private void sleep(long sleepDuration) {
        try {
            Thread.sleep(sleepDuration);
        } catch (InterruptedException e2) {
            // ignore
        }
    }

}
