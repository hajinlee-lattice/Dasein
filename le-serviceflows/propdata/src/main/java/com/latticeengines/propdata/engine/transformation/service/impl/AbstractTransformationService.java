package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
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
    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;
    private static final String TRANSFORMATION_CONF = ".CONF";
    protected static final String HDFS_PATH_SEPARATOR = "/";
    protected ObjectMapper om = new ObjectMapper();

    abstract TransformationProgressEntityMgr getProgressEntityMgr();

    abstract void executeDataFlow(TransformationProgress progress, String workflowDir);

    abstract Date checkTransformationConfigurationValidity(TransformationConfiguration transformationConfiguration);

    abstract TransformationProgress transformHook(TransformationProgress progress);

    abstract TransformationConfiguration createNewConfiguration(String latestBaseVersion, String newLatestVersion);

    abstract String getRootBaseSourceDirPath();

    abstract TransformationConfiguration readTransformationConfigurationObject(String confStr) throws IOException;

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
            TransformationProgress progress) throws IOException, JsonGenerationException, JsonMappingException {
        HdfsUtils hdfsUtil = new HdfsUtils();
        hdfsUtil.writeToFile(getYarnConfiguration(), getWorkflowDirForConf(getSource(), progress) + TRANSFORMATION_CONF,
                om.writeValueAsString(transformationConfiguration));
    }

    protected TransformationConfiguration readTransformationConfig(TransformationProgress progress)
            throws IOException, JsonGenerationException, JsonMappingException {
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
        return getHdfsPathBuilder().constructTransformationSourceDir(source).toString();
    }

    protected String sourceDirInHdfs(TransformationProgress progress) {
        return getHdfsPathBuilder().constructTransformationSourceDir(getSource(), progress.getVersion()).toString();
    }

    protected String workflowDirInHdfs(TransformationProgress progress) {
        return getHdfsPathBuilder().constructWorkFlowDir(getSource(), progress.getRootOperationUID()).toString();
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
    public boolean isNewDataAvailable() {
        return true;
    }

    protected String findLatestVersionInDir(String dir) throws IOException {
        List<String> files = findSortedVersionsInDir(dir);
        if (!CollectionUtils.isEmpty(files)) {
            String latestFilePath = files.get(files.size() - 1);
            if (latestFilePath.contains(HDFS_PATH_SEPARATOR)) {
                return latestFilePath.substring(latestFilePath.lastIndexOf(HDFS_PATH_SEPARATOR),
                        latestFilePath.length());
            }

            return latestFilePath;
        }
        return null;
    }

    protected List<String> findSortedVersionsInDir(String dir) throws IOException {
        List<String> files = HdfsUtils.getFilesForDir(getYarnConfiguration(), dir);
        if (!CollectionUtils.isEmpty(files)) {
            java.util.Collections.sort(files);
            return files;
        }
        return null;
    }
}
