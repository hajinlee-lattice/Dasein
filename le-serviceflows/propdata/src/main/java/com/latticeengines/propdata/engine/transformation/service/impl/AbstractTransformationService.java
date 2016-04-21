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
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgressStatus;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.util.LoggingUtils;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.TransformationService;

public abstract class AbstractTransformationService extends TransformationServiceBase implements TransformationService {
    private static final String DOT_CONF = ".CONF";

    private static Logger LOG = LogManager.getLogger(AbstractTransformationService.class);

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected YarnConfiguration yarnConfiguration;

    private static final String TRANSFORMATION_CONF = DOT_CONF;
    protected static final String HDFS_PATH_SEPARATOR = "/";
    protected JsonUtils om = new JsonUtils();

    abstract TransformationProgressEntityMgr getProgressEntityMgr();

    abstract void executeDataFlow(TransformationProgress progress, String workflowDir);

    abstract Date checkTransformationConfigurationValidity(TransformationConfiguration transformationConfiguration);

    abstract TransformationProgress transformHook(TransformationProgress progress);

    abstract TransformationConfiguration createNewConfiguration(String latestBaseVersion, String newLatestVersion);

    abstract String getRootBaseSourceDirPath();

    abstract TransformationConfiguration readTransformationConfigurationObject(String confStr) throws IOException;

    abstract String workflowAvroDir(TransformationProgress progress);

    @Override
    public TransformationProgress startNewProgress(TransformationConfiguration transformationConfiguration,
            String creator) {
        checkTransformationConfigurationValidity(transformationConfiguration);
        TransformationProgress progress;
        try {
            progress = getProgressEntityMgr().insertNewProgress(getSource(), transformationConfiguration.getVersion(),
                    creator);
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
        HdfsUtils hdfsUtil = new HdfsUtils();
        hdfsUtil.writeToFile(getYarnConfiguration(), getWorkflowDirForConf(getSource(), progress) + TRANSFORMATION_CONF,
                om.serialize(transformationConfiguration));
    }

    protected TransformationConfiguration readTransformationConfig(TransformationProgress progress) throws IOException {
        HdfsUtils hdfsUtil = new HdfsUtils();
        String confFilePath = getWorkflowDirForConf(getSource(), progress) + TRANSFORMATION_CONF;
        String confStr = hdfsUtil.getHdfsFileContents(getYarnConfiguration(), confFilePath);
        return readTransformationConfigurationObject(confStr);
    }

    protected String getWorkflowDirForConf(Source source, TransformationProgress progress) {
        return hdfsPathBuilder.constructWorkFlowDir(source, progress.getRootOperationUID()).toString();
    }

    @Override
    public TransformationProgress transform(TransformationProgress progress) {
        // check request context
        if (!checkProgressStatus(progress)) {
            return progress;
        }

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

    protected String sourceDirInHdfs(TransformationProgress progress) {
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

    private String incrementalDataDirInHdfs(TransformationProgress progress) {
        Path incrementalDataDir = getHdfsPathBuilder().constructRawIncrementalDir(getSource(), progress.getEndDate());
        return incrementalDataDir.toString();
    }

    @Override
    public boolean isNewDataAvailable(TransformationConfiguration transformationConfiguration) {
        return true;
    }

    protected String findLatestVersionInDir(String dir) throws IOException {
        List<String> fullVersionDirs = findSortedVersionsInDir(dir);
        if (!CollectionUtils.isEmpty(fullVersionDirs)) {
            return fullVersionDirs.get(0);
        }
        return null;
    }

    private String getVersionFromPath(String fullVersionPath) {
        return fullVersionPath.substring(fullVersionPath.lastIndexOf(HDFS_PATH_SEPARATOR) + 1);
    }

    protected List<String> findSortedVersionsInDir(String dir) throws IOException {
        List<String> versionDirs = new ArrayList<>();

        if (HdfsUtils.fileExists(getYarnConfiguration(), dir)) {
            List<String> fullVersionDirs = HdfsUtils.getFilesForDir(getYarnConfiguration(), dir);
            if (!CollectionUtils.isEmpty(fullVersionDirs)) {
                java.util.Collections.sort(fullVersionDirs);
                java.util.Collections.reverse(fullVersionDirs);

                for (String fullVersionDir : fullVersionDirs) {
                    versionDirs.add(getVersionFromPath(fullVersionDir));
                }

                return versionDirs;
            }
        }
        return versionDirs;
    }

    protected boolean doPostProcessing(TransformationProgress progress, String workflowDir) {
        String avroWorkflowDir = workflowAvroDir(progress);

        // extract schema
        try {
            extractSchema(progress, avroWorkflowDir);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to extract schema of " + getSource().getSourceName() + " avsc.", e);
            return false;
        }

        // copy result to source version dir
        try {
            String sourceDir = sourceDirInHdfs(progress);
            HdfsUtils.rmdir(yarnConfiguration, sourceDir);
            HdfsUtils.copyFiles(yarnConfiguration, avroWorkflowDir, sourceDir);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to copy pivoted data to Snapshot folder.", e);
            return false;
        }

        // delete intermediate data
        deleteFSEntry(progress, avroWorkflowDir);
        deleteFSEntry(progress, workflowDir);
        deleteFSEntry(progress, workflowDir + DOT_CONF);

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
}
