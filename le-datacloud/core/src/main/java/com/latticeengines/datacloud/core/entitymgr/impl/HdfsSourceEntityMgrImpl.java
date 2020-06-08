package com.latticeengines.datacloud.core.entitymgr.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.CollectedSource;
import com.latticeengines.datacloud.core.source.HasSqlPresence;
import com.latticeengines.datacloud.core.source.IngestedRawSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetaDataTableUtils;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Component("hdfsSourceEntityMgr")
public class HdfsSourceEntityMgrImpl implements HdfsSourceEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(HdfsSourceEntityMgrImpl.class);

    private static final long SLEEP_DURATION_IN_EXCEPTION_HADLING = 1000L;

    private static final String SUCCESS_FILE_SUFFIX = "_SUCCESS";

    private static final String AVRO_FILE_EXTENSION = ".avro";

    private static final String WILD_CARD = "*";

    private static final String HDFS_PATH_SEPARATOR = "/";

    @Inject
    HdfsPathBuilder hdfsPathBuilder;

    @Inject
    YarnConfiguration yarnConfiguration;

    @Inject
    private MetadataProxy metadataProxy;

    @Override
    public List<String> getAllSources() {
        String basePath = hdfsPathBuilder.constructSourceBaseDir().toString();
        try {
            List<FileStatus> status = HdfsUtils.getFileStatusesForDir(yarnConfiguration, basePath, null);
            List<String> sources = new ArrayList<>();
            status.forEach(s -> {
                if (s.isDirectory()) {
                    sources.add(s.getPath().getName());
                }
            });
            return sources;
        } catch (IOException e) {
            throw new RuntimeException("Fail to scan hdfs path " + basePath);
        }

    }

    @Override
    public String getCurrentVersion(Source source) {
        if (source instanceof TableSource) {
            return HdfsPathBuilder.dateFormat.format(new Date());
        } else {
            String versionFile = hdfsPathBuilder.constructVersionFile(source).toString();
            int retries = 0;
            while (retries++ < 3) {
                try {
                    String version = HdfsUtils.getHdfsFileContents(yarnConfiguration, versionFile);
                    version = version.replace("\n", "");
                    return StringUtils.trim(version);
                } catch (Exception e) {
                    SleepUtils.sleep(SLEEP_DURATION_IN_EXCEPTION_HADLING);
                }
            }
        }
        throw new RuntimeException("Could not determine the current version of source " + source.getSourceName());
    }

    @Override
    public String getCurrentVersion(String sourceName) {
        String versionFile = hdfsPathBuilder.constructVersionFile(sourceName).toString();
        int retries = 0;
        while (retries++ < 3) {
            try {
                if (!HdfsUtils.fileExists(yarnConfiguration, versionFile)) {
                    return null;
                }
                String version = HdfsUtils.getHdfsFileContents(yarnConfiguration, versionFile);
                version = version.replace("\n", "");
                return StringUtils.trim(version);
            } catch (Exception e) {
                SleepUtils.sleep(SLEEP_DURATION_IN_EXCEPTION_HADLING);
            }
        }
        throw new RuntimeException("Could not determine the current version of source " + sourceName);
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

    // does not work with ingestion version files
    @Override
    public synchronized void setCurrentVersion(String source, String version) {
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
                SleepUtils.sleep(SLEEP_DURATION_IN_EXCEPTION_HADLING);
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

        String path = hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), version).toString();
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
        if (source instanceof TableSource) {
            return ((TableSource) source).getTable();
        }
        String path = hdfsPathBuilder.constructTransformationSourceDir(source, version).toString();
        if (source instanceof HasSqlPresence) {
            return MetaDataTableUtils.createTable(yarnConfiguration, ((HasSqlPresence) source).getSqlTableName(),
                    path + HDFS_PATH_SEPARATOR + WILD_CARD + AVRO_FILE_EXTENSION, true);
        } else {
            return MetaDataTableUtils.createTable(yarnConfiguration, source.getSourceName(),
                    path + HDFS_PATH_SEPARATOR + WILD_CARD + AVRO_FILE_EXTENSION, true);
        }
    }

    @Override
    public Table getTableAtVersions(Source source, List<String> versions) {
        if (source instanceof CollectedSource || source instanceof TableSource) {
            throw new UnsupportedOperationException(
                    "Do not know how to extract versioned table for " + CollectedSource.class + " or "
                            + TableSource.class);
        }
        List<String> paths = new ArrayList<>();
        if (CollectionUtils.isEmpty(versions)) {
            paths.add(hdfsPathBuilder.constructTransformationSourceDir(source, null).toString());
        } else {
            versions.forEach(version -> {
                paths.add(hdfsPathBuilder.constructTransformationSourceDir(source, version).toString());
            });
        }
        if (source instanceof HasSqlPresence) {
            return MetaDataTableUtils.createTable(yarnConfiguration, ((HasSqlPresence) source).getSqlTableName(),
                    paths.toArray(new String[paths.size()]), source.getPrimaryKey(), true);
        } else {
            return MetaDataTableUtils.createTable(yarnConfiguration, source.getSourceName(),
                    paths.toArray(new String[paths.size()]), source.getPrimaryKey(), true);
        }
    }

    @Override
    public Schema getAvscSchemaAtVersion(Source source, String version) {
        if (source instanceof TableSource) {
            TableSource tableSource = (TableSource) source;
            Table table = tableSource.getTable();
            if (table != null && CollectionUtils.isNotEmpty(table.getAttributes())) {
                return TableUtils.createSchema(AvroUtils.getAvroFriendlyString(table.getName()), table);
            } else {
                String path = hdfsPathBuilder.constructTableSchemaFilePath(tableSource.getTable().getName(),
                        tableSource.getCustomerSpace(), tableSource.getTable().getNamespace()).toString();
                return getAvscSchemaAtVersion(tableSource.getTable().getName(), version, path);
            }
        } else {
            return getAvscSchemaAtVersion(source.getSourceName(), version);
        }
    }

    @Override
    public Schema getAvscSchemaAtVersion(String sourceName, String version) {
        String path = hdfsPathBuilder.constructSchemaFile(sourceName, version).toString();
        return getAvscSchemaAtVersion(sourceName, version, path);
    }

    private Schema getAvscSchemaAtVersion(String sourceName, String version, String path) {
        boolean avscExists;
        try {
            avscExists = HdfsUtils.fileExists(yarnConfiguration, path);
        } catch (IOException e) {
            log.error("Failed to check existence of avsc file.", e);
            return null;
        }
        if (avscExists) {
            Schema.Parser parser = new Schema.Parser();
            try {
                InputStream is = HdfsUtils.getInputStream(yarnConfiguration, path);
                Schema parsed = parser.parse(is);
                log.info("Parsed avsc schema in " + path);
                return parsed;
            } catch (Exception e) {
                log.error("Failed to extract schema from avsc file " + path, e);
                return null;
            }
        } else {
            log.warn(String.format("AVSC for source %s at version %s does not exist.", sourceName, version));
            return null;
        }
    }

    @Override
    public TableSource materializeTableSource(String tableName, CustomerSpace customerSpace) {
        String avroDir = hdfsPathBuilder.constructTablePath(tableName, customerSpace, "").toString();
        Table table = MetadataConverter.getTable(yarnConfiguration, avroDir);
        table.setName(tableName);
        return new TableSource(table, customerSpace);
    }

    @Override
    public TableSource materializeTableSource(TableSource tableSource, Long count) {
        return materializeTableSource(tableSource, count, false);
    }

    @Override
    public TableSource materializeTableSource(TableSource tableSource, Long count, boolean hasSparkPartition) {
        boolean expandBucketed = tableSource.isExpandBucketedAttrs();
        String tableName = tableSource.getTable().getName();
        CustomerSpace customerSpace = tableSource.getCustomerSpace();
        Table table;
        String avroDir = hdfsPathBuilder.constructTablePath(tableName, customerSpace, "").toString();
        String avscPath = hdfsPathBuilder.constructTableSchemaFilePath(tableName, customerSpace, "").toString();
        if (expandBucketed) {
            table = MetadataConverter.getBucketedTableFromSchemaPath(yarnConfiguration, avroDir, avscPath,
                    tableSource.getSinglePrimaryKey(), tableSource.getLastModifiedKey());
            // TODO support spark partition version
        } else {
            if (hasSparkPartition) {
                log.info("convert spark partition directory {} to table", avroDir);
                table = MetadataConverter.getTableFromDir(yarnConfiguration, avroDir,
                        tableSource.getSinglePrimaryKey(), tableSource.getLastModifiedKey(), count != null);
            } else {
                // get table by glob
                table = MetadataConverter.getTable(yarnConfiguration, avroDir, tableSource.getSinglePrimaryKey(),
                        tableSource.getLastModifiedKey(), count != null);
            }
            try {
                boolean avscExists = HdfsUtils.fileExists(yarnConfiguration, avscPath);
                if (avscExists) {
                    List<Extract> extracts = table.getExtracts();
                    InputStream is = HdfsUtils.getInputStream(yarnConfiguration, avscPath);
                    Schema schema = new Schema.Parser().parse(is);
                    Table table2 = MetadataConverter.getTable(schema, extracts, tableSource.getSinglePrimaryKey(), //
                            tableSource.getLastModifiedKey(), false);
                    table.setAttributes(table2.getAttributes());
                    log.info("Overwrite table schema by provided avsc at " + avscPath);
                }
            } catch (IOException e) {
                log.warn("Failed to overwrite table schema by provided avsc.");
            }

        }
        table.setName(tableName);
        if (count != null) {
            table.getExtracts().get(0).setProcessedRecords(count);
        }
        return new TableSource(table, customerSpace);
    }

    @Override
    public List<String> getVersions(Source source) {
        String basePath;
        if (source instanceof TableSource) {
            throw new UnsupportedOperationException("Not support getting versions for TableSource");
        } else if (source instanceof IngestionSource) {
            basePath = hdfsPathBuilder.constructIngestionDir(((IngestionSource) source).getIngestionName()).toString();
        } else {
            basePath = hdfsPathBuilder.constructSnapshotRootDir(source.getSourceName()).toString();
        }
        List<String> versions = new ArrayList<>();
        try {
            if (!HdfsUtils.isDirectory(yarnConfiguration, basePath)) {
                return versions;
            }
            List<String> dirs = HdfsUtils.getFilesForDir(yarnConfiguration, basePath);
            if (CollectionUtils.isEmpty(dirs)) {
                return versions;
            }
            for (String dir : dirs) {
                if (HdfsUtils.isDirectory(yarnConfiguration, dir)) {
                    String version = dir.substring(dir.lastIndexOf(HDFS_PATH_SEPARATOR) + 1);
                    versions.add(version);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to get all versions for " + source.getSourceName(), e);
        }
        return versions;
    }

    @Override
    public String getRequest(Source source, String requestName) {
        String request = null;
        try {
            String requestFile = hdfsPathBuilder.constructSourceDir(source.getSourceName()).append("requests")
                    .append(requestName + ".json").toString();
            request = HdfsUtils.getHdfsFileContents(yarnConfiguration, requestFile);
        } catch (Exception e) {
            log.error("Failed to load request " + requestName + " from source " + source.getSourceName(), e);
            request = null;
        }
        return request;
    }

    @Override
    public boolean saveReport(Source source, String reportName, String version, String report) {

        try {
            Path reportPath = hdfsPathBuilder.constructSourceDir(source.getSourceName()).append("reports");

            if (!HdfsUtils.fileExists(yarnConfiguration, reportPath.toString())) {
                HdfsUtils.mkdir(yarnConfiguration, reportPath.toString());
            }

            reportPath = reportPath.append(reportName);
            if (!HdfsUtils.fileExists(yarnConfiguration, reportPath.toString())) {
                HdfsUtils.mkdir(yarnConfiguration, reportPath.toString());
            }

            String reportFile = reportPath.append(version + ".json").toString();
            if (HdfsUtils.fileExists(yarnConfiguration, reportFile)) {
                HdfsUtils.rmdir(yarnConfiguration, reportFile);
            }
            HdfsUtils.writeToFile(yarnConfiguration, reportFile, report);
        } catch (Exception e) {
            log.error("Failed to save report", e);
            return false;
        }

        return true;
    }

    @Override
    public Table getCollectedTableSince(IngestedRawSource source, Date earliest) {
        String firstVersion = HdfsPathBuilder.dateFormat.format(earliest);
        return getCollectedTableSince(source, firstVersion);
    }

    @Override
    public Table getCollectedTableSince(IngestedRawSource source, String firstVersion) {
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
        return MetaDataTableUtils.createTable(yarnConfiguration, source.getSourceName(),
                avroPaths.toArray(new String[avroPaths.size()]), source.getPrimaryKey());
    }

    @Override
    public Long count(Source source, String version) {
        try {
            String avroDir;
            if (source instanceof CollectedSource || source instanceof IngestedRawSource) {
                String rawDir = hdfsPathBuilder.constructRawDir(source).toString();
                avroDir = rawDir + HDFS_PATH_SEPARATOR + version;
            } else if (source instanceof TableSource) {
                String tableName = ((TableSource) source).getTable().getName();
                CustomerSpace customerSpace = ((TableSource) source).getCustomerSpace();
                avroDir = hdfsPathBuilder.constructTablePath(tableName, customerSpace, "").toString();
            } else {
                avroDir = hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), version).toString();
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

    @Override
    public boolean checkSourceExist(Source source, String version) {
        boolean sourceExists = false;
        String versionDir = null;
        if (source instanceof IngestionSource) {
            versionDir = hdfsPathBuilder.constructIngestionDir(((IngestionSource) source).getIngestionName(), version)
                    .toString();
        } else {
            versionDir = hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), version)
                    .toString();
        }
        try {
            String success = versionDir + HDFS_PATH_SEPARATOR + SUCCESS_FILE_SUFFIX;
            if (HdfsUtils.isDirectory(yarnConfiguration, versionDir)
                    && HdfsUtils.fileExists(yarnConfiguration, success)) {
                sourceExists = true;
            }
        } catch (Exception e) {
            log.info(String.format("Failed to check %s %s @version %s in HDFS", source.getSourceName(),
                    (source instanceof IngestionSource ? ((IngestionSource) source).getIngestionName() : ""), version));
        }
        return sourceExists;
    }

    @Override
    public boolean checkSourceExist(Source source) {
        String sourceDir = null;
        if (source instanceof IngestionSource) {
            sourceDir = hdfsPathBuilder
                    .constructIngestionDir(((IngestionSource) source).getIngestionName())
                    .toString();
        } else {
            sourceDir = hdfsPathBuilder.constructSourceDir(source).toString();
        }
        try {
            if (HdfsUtils.isDirectory(yarnConfiguration, sourceDir)) {
                return true;
            }
        } catch (Exception e) {
            log.warn("Failed to check " + source + " at " + sourceDir, e);
        }
        return false;
    }

    @Override
    public boolean checkSourceExist(String source) {
        boolean sourceExists = false;
        String sourceDir = hdfsPathBuilder.constructSourceDir(source).toString();
        try {
            if (HdfsUtils.isDirectory(yarnConfiguration, sourceDir)) {
                sourceExists = true;
            }
        } catch (Exception e) {
            log.warn("Failed to check " + source + " at " + sourceDir, e);
        }
        return sourceExists;
    }

    @Override
    public void initiateSource(Source source) {
        String sourceDir = hdfsPathBuilder.constructSourceDir(source.getSourceName()).toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, sourceDir)) {
                return;
            } else {
                HdfsUtils.mkdir(yarnConfiguration, sourceDir);
            }
        } catch (Exception e) {
            log.error("Failed to initiate source " + source.getSourceName() + " in HDFS");
        }

    }

    @Override
    public void deleteSource(Source source) {
        String sourceDir = hdfsPathBuilder.constructSourceDir(source.getSourceName()).toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, sourceDir)) {
                HdfsUtils.rmdir(yarnConfiguration, sourceDir);
            }
        } catch (Exception e) {
            log.error("Failed to delete source" + source.getSourceName() + " in HDFS");
        }
    }


    @Override
    public synchronized void deleteSource(String source, String version) {
        String path = hdfsPathBuilder.constructSnapshotDir(source, version).toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, path)) {
                HdfsUtils.rmdir(yarnConfiguration, path);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete " + source + " snapshot at version " + version, e);
        }

        path = hdfsPathBuilder.constructSchemaFile(source, version).toString();
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, path)) {
                HdfsUtils.rmdir(yarnConfiguration, path);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete " + source + " schema at version " + version, e);
        }

        log.info("Deleted " + source + " at version " + version);
    }

}
