package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.CollectionDataFlowKeys;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.util.LoggingUtils;
import com.latticeengines.propdata.engine.common.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.propdata.engine.transformation.ProgressHelper;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

public abstract class AbstractTransformationService<T extends TransformationConfiguration>
        implements TransformationService<T> {

    private static Logger LOG = LogManager.getLogger(AbstractTransformationService.class);

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
    protected YarnConfiguration yarnConfiguration;

    @Autowired
    protected SourceColumnEntityMgr sourceColumnEntityMgr;

    @Autowired
    private ProgressHelper progressHelper;

    abstract Log getLogger();

    abstract Date checkTransformationConfigurationValidity(T transformationConfiguration);

    abstract T createNewConfiguration(List<String> latestBaseVersions, String newLatestVersion,
            List<SourceColumn> sourceColumns);

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
            progress = progressEntityMgr.findEarliestFailureUnderMaxRetry(getSource(), transformationConfiguration.getVersion());

            if (progress == null) {
                progress = progressEntityMgr.insertNewProgress(getSource(), transformationConfiguration.getVersion(), creator);
            } else {
                LOG.info("Retrying " + progress.getRootOperationUID());
                progress.setNumRetries(progress.getNumRetries() + 1);
                progress = progressEntityMgr.updateStatus(progress, ProgressStatus.NEW);
            }
            writeTransConfOutsideWorkflow(transformationConfiguration, progress);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to start a new progress for " + getSource(), e);
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
        progressEntityMgr.updateStatus(progress, ProgressStatus.TRANSFORMING);
        LoggingUtils.logInfo(getLogger(), progress, "Start transforming ...");

        transformHook(progress, transformationConfiguration);

        LoggingUtils.logInfoWithDuration(getLogger(), progress, "transformed.", startTime);
        return progressEntityMgr.updateStatus(progress, ProgressStatus.FINISHED);
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
                java.util.Collections.sort(fullVersionDirs);
                java.util.Collections.reverse(fullVersionDirs);

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

    private void deleteFSEntry(TransformationProgress progress, String entryPath) {
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
        LOG.info("Checking for success flag in " + pathForSuccessFlagLookup);
        return HdfsUtils.fileExists(yarnConfiguration, successFlagPath);
    }

    protected boolean isAlreadyBeingProcessed(Source source, String version) {
        return progressEntityMgr.findRunningProgress(source, version) != null;
    }

    @Override
    public T createTransformationConfiguration(List<String> versionsToProcess) {
        return createNewConfiguration(versionsToProcess, versionsToProcess.get(0),
                sourceColumnEntityMgr.getSourceColumns(getSource().getSourceName()));
    }

    protected void setAdditionalDetails(String newLatestVersion, List<SourceColumn> sourceColumns, T configuration) {
        configuration.setSourceName(getSource().getSourceName());
        Map<String, String> sourceConfigurations = new HashMap<>();
        configuration.setSourceConfigurations(sourceConfigurations);
        configuration.setVersion(newLatestVersion);
        configuration.setSourceColumns(sourceColumns);
    }

    protected boolean doPostProcessing(TransformationProgress progress, String workflowDir) {
        String avroWorkflowDir = finalWorkflowOuputDir(progress);
        try {

            // extract schema
            try {
                extractSchema(progress, avroWorkflowDir);
            } catch (Exception e) {
                updateStatusToFailed(progress, "Failed to extract schema of " + getSource().getSourceName() + " avsc.",
                        e);
                return false;
            }

            // copy result to source version dir
            try {
                String sourceDir = sourceVersionDirInHdfs(progress);
                deleteFSEntry(progress, sourceDir);
                String currentMaxVersion = null;

                for (String avroFilePath : HdfsUtils.getFilesByGlob(yarnConfiguration,
                        avroWorkflowDir + HDFS_PATH_SEPARATOR + AVRO_REGEX)) {
                    if (!HdfsUtils.isDirectory(yarnConfiguration, sourceDir)) {
                        HdfsUtils.mkdir(yarnConfiguration, sourceDir);
                    }
                    String avroFileName = new Path(avroFilePath).getName();
                    LOG.info("Move file from " + avroFilePath + " to " + new Path(sourceDir, avroFileName).toString());
                    HdfsUtils.moveFile(yarnConfiguration, avroFilePath, new Path(sourceDir, avroFileName).toString());
                }

                try {
                    currentMaxVersion = hdfsSourceEntityMgr.getCurrentVersion(getSource());
                } catch (Exception e) {
                    // We can log this exception and ignore this error as
                    // probably file does not even exists
                    LOG.debug("Could not read version file", e);
                }
                // overwrite max version if progress's version is higher
                if (currentMaxVersion == null || progress.getVersion().compareTo(currentMaxVersion) > 0) {
                    hdfsSourceEntityMgr.setCurrentVersion(getSource(), progress.getVersion());
                }

                HdfsUtils.writeToFile(yarnConfiguration, sourceDir + HDFS_PATH_SEPARATOR + SUCCESS_FLAG, "");
            } catch (Exception e) {
                updateStatusToFailed(progress, "Failed to copy pivoted data to Snapshot folder.", e);
                return false;
            }

        } finally {
            // delete intermediate data
            deleteFSEntry(progress, avroWorkflowDir);
            deleteFSEntry(progress, workflowDir);
        }

        return false;
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
        return hdfsPathBuilder.constructSnapshotDir(getSource(), getVersionString(progress)).toString();
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

    protected void extractSchema(TransformationProgress progress, String avroDir) throws Exception {
        String version = getVersion(progress);
        String avscPath = hdfsPathBuilder.constructSchemaFile(getSource(), version).toString();
        if (HdfsUtils.fileExists(yarnConfiguration, avscPath)) {
            HdfsUtils.rmdir(yarnConfiguration, avscPath);
        }

        List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration,
                avroDir + HDFS_PATH_SEPARATOR + WILD_CARD + AVRO_EXTENSION);
        if (files.size() > 0) {
            String avroPath = files.get(0);
            if (HdfsUtils.fileExists(yarnConfiguration, avroPath)) {
                Schema schema = AvroUtils.getSchema(yarnConfiguration, new org.apache.hadoop.fs.Path(avroPath));
                HdfsUtils.writeToFile(yarnConfiguration, avscPath, schema.toString());
            }
        } else {
            throw new IllegalStateException("No avro file found at " + avroDir);
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

    protected String finalWorkflowOuputDir(TransformationProgress progress) {
        // Firehose transformation has special setting. Otherwise, it is the
        // default dataFlowDir
        return initialDataFlowDirInHdfs(progress);
    }

    protected String getVersion(TransformationProgress progress) {
        return progress.getVersion();
    }

}
