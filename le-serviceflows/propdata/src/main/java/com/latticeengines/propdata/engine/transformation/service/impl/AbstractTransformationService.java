package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.util.LoggingUtils;
import com.latticeengines.propdata.engine.common.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

public abstract class AbstractTransformationService<T extends TransformationConfiguration>
        extends TransformationServiceBase implements TransformationService<T> {
    private static final String SUCCESS_FLAG = "_SUCCESS";

    private static Logger LOG = LogManager.getLogger(AbstractTransformationService.class);

    private static final String TRANSFORMATION_CONF = "_CONF";

    private static final String AVRO_REGEX = "*.avro";

    protected static final String HDFS_PATH_SEPARATOR = "/";

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected TransformationProgressEntityMgr transformationProgressEntityMgr;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    protected YarnConfiguration yarnConfiguration;

    @Autowired
    protected SourceColumnEntityMgr sourceColumnEntityMgr;

    @Override
    TransformationProgressEntityMgr getProgressEntityMgr() {
        return transformationProgressEntityMgr;
    }

    abstract Date checkTransformationConfigurationValidity(T transformationConfiguration);

    abstract TransformationProgress transformHook(TransformationProgress progress, T transformationConfiguration);

    abstract T createNewConfiguration(List<String> latestBaseVersions, String newLatestVersion,
            List<SourceColumn> sourceColumns);

    abstract List<String> getRootBaseSourceDirPaths();

    abstract T readTransformationConfigurationObject(String confStr) throws IOException;

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
            } else if (transformationProgressEntityMgr.findRunningProgress(source, expectedCurrentVersion) != null) {
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
            progress = getProgressEntityMgr().findEarliestFailureUnderMaxRetry(getSource(),
                    transformationConfiguration.getVersion());

            if (progress == null) {
                progress = getProgressEntityMgr().insertNewProgress(getSource(),
                        transformationConfiguration.getVersion(), creator);
            } else {
                LOG.info("Retrying " + progress.getRootOperationUID());
                progress.setNumRetries(progress.getNumRetries() + 1);
                progress = getProgressEntityMgr().updateStatus(progress, ProgressStatus.NEW);
            }
            writeTransformationConfig(transformationConfiguration, progress);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25011, transformationConfiguration.getSourceName(), e);
        }
        LoggingUtils.logInfo(getLogger(), progress,
                "Started a new progress with version=" + transformationConfiguration.getVersion());
        return progress;
    }

    protected void writeTransformationConfig(T transformationConfiguration, TransformationProgress progress)
            throws IOException {
        transformationConfiguration.setRootOperationId(progress.getRootOperationUID());
        HdfsUtils.writeToFile(getYarnConfiguration(),
                finalWorkflowOuputDir(progress) + HDFS_PATH_SEPARATOR + TRANSFORMATION_CONF,
                JsonUtils.serialize(transformationConfiguration));
    }

    protected T readTransformationConfig(TransformationProgress progress) throws IOException {
        String confFilePath = finalWorkflowOuputDir(progress) + HDFS_PATH_SEPARATOR + TRANSFORMATION_CONF;
        String confStr = HdfsUtils.getHdfsFileContents(getYarnConfiguration(), confFilePath);
        return readTransformationConfigurationObject(confStr);
    }

    @Override
    public TransformationProgress transform(TransformationProgress progress, T transformationConfiguration) {
        // update status
        logIfRetrying(progress);
        long startTime = System.currentTimeMillis();
        getProgressEntityMgr().updateStatus(progress, ProgressStatus.TRANSFORMING);
        LoggingUtils.logInfo(getLogger(), progress, "Start transforming ...");

        transformHook(progress, transformationConfiguration);

        LoggingUtils.logInfoWithDuration(getLogger(), progress, "transformed.", startTime);
        return getProgressEntityMgr().updateStatus(progress, ProgressStatus.FINISHED);
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

        if (HdfsUtils.fileExists(getYarnConfiguration(), dir)) {
            List<String> fullVersionDirs = HdfsUtils.getFilesForDir(getYarnConfiguration(), dir);
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
        return transformationProgressEntityMgr.findRunningProgress(source, version) != null;
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
}
