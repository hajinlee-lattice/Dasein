package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.TableSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.LoggingUtils;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.service.HiveTableService;
import com.latticeengines.datacloud.etl.transformation.ProgressHelper;
import com.latticeengines.datacloud.etl.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.dataflow.CollectionDataFlowKeys;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public abstract class AbstractTransformationService<T extends TransformationConfiguration>
        implements TransformationService<T> {

    private static Logger log = LoggerFactory.getLogger(AbstractTransformationService.class);

    private static final String SUCCESS_FLAG = "_SUCCESS";
    private static final String TRANSFORMATION_CONF = "_CONF";
    private static final String AVRO_REGEX = "*.avro";
    protected static final String HDFS_PATH_SEPARATOR = "/";
    private static final String AVRO_EXTENSION = ".avro";
    private static final String WILD_CARD = "*";
    protected static final String VERSION = "VERSION";

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected TransformationProgressEntityMgr progressEntityMgr;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected SourceColumnEntityMgr sourceColumnEntityMgr;

    @Autowired
    private ProgressHelper progressHelper;

    @Autowired
    private HiveTableService hiveTableService;

    @Autowired
    private MetadataProxy metadataProxy;

    abstract Logger getLogger();

    abstract Date checkTransformationConfigurationValidity(T transformationConfiguration);

    public abstract T createTransformationConfiguration(List<String> baseVersionsToProcess, String targetVersion);

    abstract T parseTransConfJsonInsideWorkflow(String confStr) throws IOException;

    protected abstract TransformationProgress transformHook(TransformationProgress progress,
            T transformationConfiguration);

    @Override
    public boolean isManualTriggerred() {
        return false;
    }

    @Override
    public List<List<String>> findUnprocessedBaseSourceVersions() {
        Source source = getSource();
        if (source instanceof DerivedSource) {
            DerivedSource derivedSource = (DerivedSource) source;
            Source[] baseSources = derivedSource.getBaseSources();
            List<String> latestBaseSourceVersions = new ArrayList<>();
            for (Source baseSource : baseSources) {
                String baseSourceVersion = hdfsSourceEntityMgr.getCurrentVersion(baseSource);
                latestBaseSourceVersions.add(baseSourceVersion);
            }
            String expectedCurrentVersion = StringUtils.join("|", latestBaseSourceVersions);
            String actualCurrentVersion = hdfsSourceEntityMgr.getCurrentVersion(derivedSource);
            if (StringUtils.isNotEmpty(actualCurrentVersion) && actualCurrentVersion.equals(expectedCurrentVersion)) {
                // latest version is already processed
                return Collections.emptyList();
            } else if (progressEntityMgr.findRunningProgress(source, expectedCurrentVersion) != null) {
                // latest version is being processed
                return Collections.emptyList();
            } else {
                return Collections.singletonList(latestBaseSourceVersions);
            }
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public TransformationProgress startNewProgress(T transformationConfiguration, String creator) {
        checkTransformationConfigurationValidity(transformationConfiguration);
        TransformationProgress progress;
        try {
            progress = progressEntityMgr.findEarliestFailureUnderMaxRetry(getSource(),
                    transformationConfiguration.getVersion());

            if (progress == null) {
                progress = progressEntityMgr.insertNewProgress(null, getSource(),
                        transformationConfiguration.getVersion(), creator);
            } else {
                log.info("Retrying " + progress.getRootOperationUID());
                progress.setNumRetries(progress.getNumRetries() + 1);
                progress = progressEntityMgr.updateStatus(progress, ProgressStatus.NEW);
            }
            writeTransConfOutsideWorkflow(transformationConfiguration, progress);
        } catch (Exception e) {
            throw new RuntimeException("Failed to start a new progress for " + getSource(), e);
        }
        LoggingUtils.logInfo(getLogger(), progress,
                "Started a new progress with version=" + transformationConfiguration.getVersion());
        return progress;
    }

    protected void writeTransConfOutsideWorkflow(T transformationConfiguration, TransformationProgress progress)
            throws IOException {
        transformationConfiguration.setRootOperationId(progress.getRootOperationUID());
        HdfsUtils.writeToFile(yarnConfiguration,
                initialDataFlowDirInHdfs(progress) + HDFS_PATH_SEPARATOR + TRANSFORMATION_CONF,
                JsonUtils.serialize(transformationConfiguration));
    }

    protected T readTransConfInsideWorkflow(TransformationProgress progress) throws IOException {
        String confFilePath = initialDataFlowDirInHdfs(progress) + HDFS_PATH_SEPARATOR + TRANSFORMATION_CONF;
        String confStr = HdfsUtils.getHdfsFileContents(yarnConfiguration, confFilePath);
        return parseTransConfJsonInsideWorkflow(confStr);
    }

    @Override
    public TransformationProgress transform(TransformationProgress progress, T transformationConfiguration) {
        // update status
        logIfRetrying(progress);
        long startTime = System.currentTimeMillis();
        LoggingUtils.logInfo(getLogger(), progress, "Start transforming ...");

        progress = transformHook(progress, transformationConfiguration);
        if (progress != null) {
            LoggingUtils.logInfoWithDuration(getLogger(), progress, "transformed.", startTime);
            return progressEntityMgr.updateStatus(progress, ProgressStatus.FINISHED);
        } else {
            return null;
        }
    }

    @Override
    public TransformationProgress finish(TransformationProgress progress) {
        return finishProgress(progress);
    }

    @Override
    public boolean isNewDataAvailable(T transformationConfiguration) {
        return true;
    }

    protected String findLatestVersionInDir(String dir, String cutoffDateVersion) throws IOException {
        List<String> fullVersionDirs = findSortedVersionsInDir(dir, cutoffDateVersion);
        if (!CollectionUtils.isEmpty(fullVersionDirs)) {
            return fullVersionDirs.get(0);
        }
        return null;
    }

    private String getVersionFromPath(String fullVersionPath) {
        return fullVersionPath.substring(fullVersionPath.lastIndexOf(HDFS_PATH_SEPARATOR) + 1);
    }

    protected List<String> findSortedVersionsInDir(String dir, String cutoffDateVersion) throws IOException {
        List<String> versionDirs = new ArrayList<>();

        if (HdfsUtils.fileExists(yarnConfiguration, dir)) {
            List<String> fullVersionDirs = HdfsUtils.getFilesForDir(yarnConfiguration, dir);
            if (!CollectionUtils.isEmpty(fullVersionDirs)) {
                Collections.sort(fullVersionDirs);
                Collections.reverse(fullVersionDirs);

                for (String fullVersionDir : fullVersionDirs) {
                    String version = getVersionFromPath(fullVersionDir);
                    if (cutoffDateVersion == null || version.compareTo(cutoffDateVersion) >= 0) {
                        versionDirs.add(version);
                    } else {
                        break;
                    }
                }

                return versionDirs;
            }
        }
        return versionDirs;
    }

    protected void deleteFSEntry(TransformationProgress progress, String entryPath) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, entryPath)) {
                HdfsUtils.rmdir(yarnConfiguration, entryPath);
            }
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to delete intermediate data.", e);
        }
    }

    protected boolean hasSuccessFlag(String pathForSuccessFlagLookup) throws IOException {
        String successFlagPath = pathForSuccessFlagLookup + HDFS_PATH_SEPARATOR + SUCCESS_FLAG;
        log.info("Checking for success flag in " + pathForSuccessFlagLookup);
        return HdfsUtils.fileExists(yarnConfiguration, successFlagPath);
    }

    protected boolean isAlreadyBeingProcessed(Source source, String version) {
        return progressEntityMgr.findRunningProgress(source, version) != null;
    }

    protected void setAdditionalDetails(String newLatestVersion, List<SourceColumn> sourceColumns, T configuration) {
        configuration.setSourceName(getSource().getSourceName());
        configuration.setSourceConfigurations(new HashMap<>());
        configuration.setVersion(newLatestVersion);
        configuration.setSourceColumns(sourceColumns);
    }

    protected boolean doPostProcessing(TransformationProgress progress, String workflowDir) {
        return doPostProcessing(progress, workflowDir, true);
    }

    protected boolean doPostProcessing(TransformationProgress progress, String workflowDir, boolean saveResult) {
        boolean status = true;
        String avroWorkflowDir = finalWorkflowOuputDir(workflowDir, progress);
        if (saveResult) {
            Source source = getSource();
            String version = progress.getVersion();
            status = saveSourceVersion(progress, source, version, avroWorkflowDir);
        }
        // delete intermediate data
        deleteFSEntry(progress, avroWorkflowDir);
        deleteFSEntry(progress, workflowDir);
        return status;
    }

    protected boolean saveSourceVersion(TransformationProgress progress, Source source, String version,
            String workflowDir) {
        return saveSourceVersion(progress, null, source, version, workflowDir, null);
    }

    boolean saveSourceVersion(TransformationProgress progress, Schema schema, Source source, String version,
            String workflowDir, Long count) {
        String finalWorkflowDir = finalWorkflowOuputDir(workflowDir, progress);
        // extract schema
        try {
            extractSchema(source, schema, version, finalWorkflowDir);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to extract schema of " + source.getSourceName() + " avsc.", e);
            return false;
        }

        // copy result to source version dir
        try {
            String sourceDir;
            if (source instanceof TableSource) {
                sourceDir = targetTableDirInHdfs((TableSource) source);
            } else {
                sourceDir = sourceVersionDirInHdfs(source, version);
            }
            deleteFSEntry(progress, sourceDir);
            String currentMaxVersion;

            log.info(String.format("Moving files from %s to %s", finalWorkflowDir, sourceDir));
            int cnt = 0;
            for (String avroFilePath : HdfsUtils.getFilesByGlob(yarnConfiguration,
                    finalWorkflowDir + HDFS_PATH_SEPARATOR + AVRO_REGEX)) {
                if (!HdfsUtils.isDirectory(yarnConfiguration, sourceDir)) {
                    HdfsUtils.mkdir(yarnConfiguration, sourceDir);
                }
                String avroFileName = new Path(avroFilePath).getName();
                HdfsUtils.moveFile(yarnConfiguration, avroFilePath, new Path(sourceDir, avroFileName).toString());
                cnt++;
            }
            log.info(String.format("Moved %d files from %s to %s", cnt, finalWorkflowDir, sourceDir));

            if (source instanceof TableSource) {
                // register table with metadata proxy
                TableSource tableSource = hdfsSourceEntityMgr.materializeTableSource((TableSource) source, count);
                metadataProxy.updateTable(tableSource.getCustomerSpace().toString(), tableSource.getTable().getName(),
                        tableSource.getTable());
            } else {
                // update current version
                try {
                    currentMaxVersion = hdfsSourceEntityMgr.getCurrentVersion(source);
                } catch (Exception e) {
                    // We can log this exception and ignore this error as
                    // probably file does not even exists
                    log.debug("Could not read version file", e);
                    currentMaxVersion = null;
                }
                // overwrite max version if progress's version is higher
                if (currentMaxVersion == null || version.compareTo(currentMaxVersion) > 0) {
                    hdfsSourceEntityMgr.setCurrentVersion(source, version);
                }
            }

            HdfsUtils.writeToFile(yarnConfiguration, sourceDir + HDFS_PATH_SEPARATOR + SUCCESS_FLAG, "");

        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to copy pivoted data to Snapshot folder.", e);
            return false;
        }

        return true;
    }

    public TransformationProgress findRunningJob() {
        return progressHelper.findRunningJob(progressEntityMgr, getSource());
    }

    public TransformationProgress findJobToRetry(String version) {
        return progressHelper.findJobToRetry(progressEntityMgr, getSource(), version);
    }

    protected boolean checkProgressStatus(TransformationProgress progress) {
        return progressHelper.checkProgressStatus(progress, getLogger());
    }

    protected void logIfRetrying(TransformationProgress progress) {
        progressHelper.logIfRetrying(progress, getLogger());
    }

    protected String snapshotDirInHdfs(TransformationProgress progress) {
        return hdfsPathBuilder.constructSnapshotDir(getSource().getSourceName(), getVersionString(progress)).toString();
    }

    protected boolean cleanupHdfsDir(String targetDir, TransformationProgress progress) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetDir)) {
                HdfsUtils.rmdir(yarnConfiguration, targetDir);
            }
        } catch (Exception e) {
            LoggingUtils.logError(getLogger(), progress, "Failed to cleanup hdfs dir " + targetDir, e);
            return false;
        }
        return true;
    }

    public String getVersionString(TransformationProgress progress) {
        return HdfsPathBuilder.dateFormat.format(progress.getCreateTime());
    }

    protected void extractSchema(Source source, Schema schema, String version, String avroDir) throws Exception {
        String avscPath;
        if (source instanceof TableSource) {
            TableSource tableSource = (TableSource) source;
            avscPath = hdfsPathBuilder.constructTableSchemaFilePath(tableSource.getTable().getName(),
                    tableSource.getCustomerSpace(), tableSource.getTable().getNamespace()).toString();
        } else {
            avscPath = hdfsPathBuilder.constructSchemaFile(source.getSourceName(), version).toString();
        }
        if (HdfsUtils.fileExists(yarnConfiguration, avscPath)) {
            HdfsUtils.rmdir(yarnConfiguration, avscPath);
        }

        Schema parsedSchema = null;
        List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration,
                avroDir + HDFS_PATH_SEPARATOR + WILD_CARD + AVRO_EXTENSION);
        if (files.size() > 0) {
            String avroPath = files.get(0);
            parsedSchema = AvroUtils.getSchema(yarnConfiguration, new Path(avroPath));
        } else {
            throw new IllegalStateException("No avro file found at " + avroDir);
        }

        if (schema != null) {
            verifySchemaCorrectness(parsedSchema, schema);
            log.info("Saving provided schema at " + avscPath);
            HdfsUtils.writeToFile(yarnConfiguration, avscPath, schema.toString());
        } else {
            log.info("Saving parsed schema at " + avscPath);
            HdfsUtils.writeToFile(yarnConfiguration, avscPath, parsedSchema.toString());
        }
    }

    private void verifySchemaCorrectness(Schema parsedSchema, Schema providedSchema) {
        List<Schema.Field> parsedFields = parsedSchema.getFields();
        List<Schema.Field> providedFields = providedSchema.getFields();
        if (parsedFields.size() != providedFields.size()) {
            throw new IllegalStateException("Provided schema has " + providedFields.size()
                    + " fields, while the schema parsed from avro has " + parsedFields.size() + " fields.");
        }
        for (int i = 0; i < parsedFields.size(); i++) {
            Schema.Field parsedField = parsedFields.get(i);
            Schema.Field provideField = providedFields.get(i);
            if (!parsedField.name().equals(provideField.name())) {
                throw new IllegalStateException("For " + i + "-th attribute, provided schema uses the name "
                        + provideField.name() + ", while the parsed schema is " + parsedField.name());
            }
            String parsedType = AvroUtils.getJavaType(AvroUtils.getType(parsedField)).getCanonicalName();
            String providedType = AvroUtils.getJavaType(AvroUtils.getType(provideField)).getCanonicalName();
            if (!parsedType.equals(providedType)) {
                throw new IllegalStateException("For " + i + "-th attribute, " +
                        "provided schema [" + provideField.name() + "] uses the type " + providedType //
                        + ", while the parsed schema [" + parsedField.name() + "] is " + parsedType);
            }
        }
    }

    public void updateStatusToFailed(TransformationProgress progress, String errorMsg, Exception e) {
        progressHelper.updateStatusToFailed(progressEntityMgr, progress, errorMsg, e, getLogger());
    }

    protected TransformationProgress finishProgress(TransformationProgress progress) {
        return progressHelper.finishProgress(progressEntityMgr, progress, getLogger());
    }

    protected String sourceDirInHdfs(Source source) {
        String sourceDirInHdfs = hdfsPathBuilder.constructTransformationSourceDir(source).toString();
        getLogger().info("sourceDirInHdfs for " + getSource().getSourceName() + " = " + sourceDirInHdfs);
        return sourceDirInHdfs;
    }

    protected String sourceVersionDirInHdfs(Source source, String version) {
        String sourceDirInHdfs = hdfsPathBuilder.constructTransformationSourceDir(source, version).toString();
        getLogger().info("sourceVersionDirInHdfs for " + source.getSourceName() + " = " + sourceDirInHdfs);
        return sourceDirInHdfs;
    }

    private String targetTableDirInHdfs(TableSource tableSource) {
        String path = hdfsPathBuilder.constructTablePath(tableSource.getTable().getName(),
                tableSource.getCustomerSpace(), tableSource.getTable().getNamespace()).toString();
        getLogger().info("targetTableDirInHdfs for " + tableSource.getSourceName() + " = " + path);
        return path;
    }

    protected String sourceVersionDirInHdfs(TransformationProgress progress) {
        String sourceDirInHdfs = hdfsPathBuilder.constructTransformationSourceDir(getSource(), progress.getVersion())
                .toString();
        getLogger().info("sourceVersionDirInHdfs for " + getSource().getSourceName() + " = " + sourceDirInHdfs);
        return sourceDirInHdfs;
    }

    protected String initialDataFlowDirInHdfs(TransformationProgress progress) {
        String workflowDir = dataFlowDirInHdfs(progress, CollectionDataFlowKeys.TRANSFORM_FLOW);
        getLogger().info("initialDataFlowDirInHdfs for " + getSource().getSourceName() + " = " + workflowDir);
        return workflowDir;
    }

    protected String dataFlowDirInHdfs(TransformationProgress progress, String dataFlowName) {
        String dataflowDir = hdfsPathBuilder.constructWorkFlowDir(getSource(), dataFlowName)
                .append(progress.getRootOperationUID()).toString();
        getLogger().info("dataFlowDirInHdfs for " + getSource().getSourceName() + " = " + dataflowDir);
        return dataflowDir;
    }

    protected String finalWorkflowOuputDir(String workflowDir, TransformationProgress progress) {
        // Firehose transformation has special setting. Otherwise, it is the passed in workflowDir
        return workflowDir;
    }

    protected String getVersion(TransformationProgress progress) {
        return progress.getVersion();
    }

    protected String createNewVersionStringFromNow() {
        return HdfsPathBuilder.dateFormat.format(new Date());
    }

}
