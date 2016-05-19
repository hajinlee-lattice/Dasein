package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgressStatus;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.util.LoggingUtils;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

public abstract class AbstractTransformationService extends TransformationServiceBase implements TransformationService {
    private static final String SUCCESS_FLAG = "_SUCCESS";

    private static Logger LOG = LogManager.getLogger(AbstractTransformationService.class);

    private static final String TRANSFORMATION_CONF = "_CONF";

    private static final String AVRO_REGEX = "*.avro";

    protected static final String HDFS_PATH_SEPARATOR = "/";

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private TransformationProgressEntityMgr transformationProgressEntityMgr;

    @Autowired
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    protected YarnConfiguration yarnConfiguration;

    abstract TransformationProgressEntityMgr getProgressEntityMgr();

    abstract void executeDataFlow(TransformationProgress progress, String workflowDir);

    abstract Date checkTransformationConfigurationValidity(TransformationConfiguration transformationConfiguration);

    abstract TransformationProgress transformHook(TransformationProgress progress);

    abstract TransformationConfiguration createNewConfiguration(List<String> latestBaseVersions,
            String newLatestVersion);

    abstract String getRootBaseSourceDirPath();

    abstract TransformationConfiguration readTransformationConfigurationObject(String confStr) throws IOException;

    abstract String workflowAvroDir(TransformationProgress progress);

    @Override
    public TransformationProgress startNewProgress(TransformationConfiguration transformationConfiguration,
            String creator) {
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
                progress = getProgressEntityMgr().updateStatus(progress, TransformationProgressStatus.NEW);
            }
            writeTransformationConfig(transformationConfiguration, progress);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25011, transformationConfiguration.getSourceName(), e);
        }
        LoggingUtils.logInfo(getLogger(), progress,
                "Started a new progress with version=" + transformationConfiguration.getVersion());
        return progress;
    }

    protected void writeTransformationConfig(TransformationConfiguration transformationConfiguration,
            TransformationProgress progress) throws IOException {
        transformationConfiguration.setRootOperationId(progress.getRootOperationUID());
        HdfsUtils.writeToFile(getYarnConfiguration(),
                getWorkflowDirForConf(getSource(), progress) + HDFS_PATH_SEPARATOR + TRANSFORMATION_CONF,
                JsonUtils.serialize(transformationConfiguration));
    }

    protected TransformationConfiguration readTransformationConfig(TransformationProgress progress) throws IOException {
        String confFilePath = getWorkflowDirForConf(getSource(), progress) + HDFS_PATH_SEPARATOR + TRANSFORMATION_CONF;
        String confStr = HdfsUtils.getHdfsFileContents(getYarnConfiguration(), confFilePath);
        return readTransformationConfigurationObject(confStr);
    }

    protected String getWorkflowDirForConf(Source source, TransformationProgress progress) {
        return hdfsPathBuilder.constructWorkFlowDir(source, progress.getRootOperationUID()).toString();
    }

    @Override
    public TransformationProgress transform(TransformationProgress progress) {
        // update status
        logIfRetrying(progress);
        long startTime = System.currentTimeMillis();
        getProgressEntityMgr().updateStatus(progress, TransformationProgressStatus.TRANSFORMING);
        LoggingUtils.logInfo(getLogger(), progress, "Start transforming ...");

        transformHook(progress);

        LoggingUtils.logInfoWithDuration(getLogger(), progress, "transformed.", startTime);
        return getProgressEntityMgr().updateStatus(progress, TransformationProgressStatus.FINISHED);
    }

    protected String sourceDirInHdfs(Source source) {
        String sourceDirInHdfs = getHdfsPathBuilder().constructTransformationSourceDir(source).toString();
        LOG.info("sourceDirInHdfs for " + getSource().getSourceName() + " = " + sourceDirInHdfs);
        return sourceDirInHdfs;
    }

    protected String sourceVersionDirInHdfs(TransformationProgress progress) {
        String sourceDirInHdfs = getHdfsPathBuilder()
                .constructTransformationSourceDir(getSource(), progress.getVersion()).toString();
        LOG.info("sourceDirInHdfs for " + getSource().getSourceName() + " = " + sourceDirInHdfs);
        return sourceDirInHdfs;
    }

    protected String workflowDirInHdfs(TransformationProgress progress) {
        String workflowDir = getHdfsPathBuilder().constructWorkFlowDir(getSource(), progress.getRootOperationUID())
                .toString();
        LOG.info("workflowDirInHdfs for " + getSource().getSourceName() + " = " + workflowDir);
        return workflowDir;
    }

    protected String dataFlowDirInHdfs(TransformationProgress progress, String dataFlowName) {
        String dataflowDir = getHdfsPathBuilder().constructWorkFlowDir(getSource(), dataFlowName)
                .append(progress.getRootOperationUID()).toString();
        LOG.info("dataFlowDirInHdfs for " + getSource().getSourceName() + " = " + dataflowDir);
        return dataflowDir;
    }

    @Override
    public TransformationProgress finish(TransformationProgress progress) {
        return finishProgress(progress);
    }

    @Override
    public boolean isNewDataAvailable(TransformationConfiguration transformationConfiguration) {
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
        String avroWorkflowDir = workflowAvroDir(progress);
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
                    String avroFileName = avroFilePath.substring(avroFilePath.lastIndexOf(HDFS_PATH_SEPARATOR) + 1);
                    HdfsUtils.copyFiles(yarnConfiguration, avroFilePath,
                            sourceDir + HDFS_PATH_SEPARATOR + avroFileName);
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
        return transformationProgressEntityMgr.findRunningProgress(source, version) == null ? false : true;
    }

    @Override
    public TransformationConfiguration createTransformationConfiguration(List<String> versionsToProcess) {
        TransformationConfiguration configuration;
        configuration = createNewConfiguration(versionsToProcess, versionsToProcess.get(0));
        return configuration;
    }
}
