package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.source.DataImportedFromHDFS;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public abstract class AbstractFirehoseTransformationService extends AbstractTransformationService {

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

        return doPostProcessing(progress, workflowDir);
    }

    abstract void uploadSourceSchema(String workflowDir) throws IOException;

    @Override
    protected String getRootBaseSourceDirPath() {
        DataImportedFromHDFS source = (DataImportedFromHDFS) getSource();
        return source.getHDFSPathToImportFrom().toString();
    }

    @Override
    public String findUnprocessedVersion() {
        Source source = getSource();
        String rootSourceDir = sourceDirInHdfs(source);
        String rootBaseSourceDir = getRootBaseSourceDirPath();
        String latestVersion = null;
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
            return latestBaseVersion;
        }

        return null;
    }

    @Override
    public TransformationConfiguration createTransformationConfiguration(String versionToProcess) {
        TransformationConfiguration configuration;
        configuration = createNewConfiguration(versionToProcess, versionToProcess);
        return configuration;
    }

    @Override
    String workflowAvroDir(TransformationProgress progress) {
        return workflowDirInHdfs(progress) + HDFS_PATH_SEPARATOR + AVRO_DIR_FOR_CONVERSION;
    }
}
