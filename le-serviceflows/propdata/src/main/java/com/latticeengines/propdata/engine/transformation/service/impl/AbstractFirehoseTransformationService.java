package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.source.DataImportedFromHDFS;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public abstract class AbstractFirehoseTransformationService extends AbstractTransformationService {
    @Autowired
    protected YarnConfiguration yarnConfiguration;

    private static final String AVRO_DIR_FOR_CONVERSION = "AVRO_DIR_FOR_CONVERSION";

    protected TransformationProgress transformHook(TransformationProgress progress) {
        if (!ingestDataFromFirehoseAndUpdateProgress(progress)) {
            return progress;
        }
        return null;
    }

    private boolean ingestDataFromFirehoseAndUpdateProgress(TransformationProgress progress) {
        String workflowDir = workflowDirInHdfs(progress);
        if (!cleanupHdfsDir(workflowDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + workflowDir, null);
            return false;
        }

        try {
            uploadSourceSchema(workflowDir);
            executeDataFlow(progress, workflowDir);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to transform data.", e);
            return false;
        }

        String arvoWorkflowDir = workflowDir + HDFS_PATH_SEPARATOR + AVRO_DIR_FOR_CONVERSION;

        // extract schema
        try {
            extractSchema(progress, arvoWorkflowDir);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to extract schema of " + getSource().getSourceName() + " avsc.", e);
            return false;
        }

        // copy result to source version dir
        try {
            String sourceDir = sourceDirInHdfs(progress);
            HdfsUtils.rmdir(yarnConfiguration, sourceDir);
            HdfsUtils.copyFiles(yarnConfiguration, arvoWorkflowDir, sourceDir);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to copy pivoted data to Snapshot folder.", e);
            return false;
        }

        // delete intermediate data
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, workflowDir)) {
                HdfsUtils.rmdir(yarnConfiguration, workflowDir);
            }
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to delete intermediate data.", e);
            return false;
        }

        return true;
    }

    abstract void uploadSourceSchema(String workflowDir) throws IOException;

    @Override
    protected String getRootBaseSourceDirPath() {
        DataImportedFromHDFS source = (DataImportedFromHDFS) getSource();
        return source.getHDFSPathToImportFrom().toString();
    }

    @Override
    public TransformationConfiguration createTransformationConfiguration() {
        Source source = getSource();
        String rootSourceDir = sourceDirInHdfs(source);
        String rootBaseSourceDir = getRootBaseSourceDirPath();
        String latestVersion = null;
        String newLatestVersion = null;
        String latestBaseVersion = null;

        try {
            latestVersion = findLatestVersionInDir(rootSourceDir.toString());
            latestBaseVersion = findLatestVersionInDir(rootBaseSourceDir.toString());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }

        if (latestBaseVersion == null) {
            return null;
        }

        if (latestVersion == null || latestBaseVersion.compareTo(latestVersion) > 0) {
            newLatestVersion = latestBaseVersion;
        }
        if (newLatestVersion != null) {

            TransformationConfiguration configuration = createNewConfiguration(latestBaseVersion, newLatestVersion);

            return configuration;
        }

        return null;
    }
}
