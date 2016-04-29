package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.source.DataImportedFromHDFS;
import com.latticeengines.propdata.core.source.Source;

public abstract class AbstractFirehoseTransformationService extends AbstractTransformationService {
    private static Logger LOG = LogManager.getLogger(AbstractFirehoseTransformationService.class);

    private static final String AVRO_DIR_FOR_CONVERSION = "AVRO_DIR_FOR_CONVERSION";

    abstract void uploadSourceSchema(String workflowDir) throws IOException;

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

    @Override
    protected String getRootBaseSourceDirPath() {
        DataImportedFromHDFS source = (DataImportedFromHDFS) getSource();
        return source.getHDFSPathToImportFrom().toString();
    }

    @Override
    public List<String> findUnprocessedVersions() {
        Source source = getSource();
        String rootSourceDir = sourceDirInHdfs(source);
        String rootBaseSourceDir = getRootBaseSourceDirPath();
        String latestVersion = null;
        String latestBaseVersion = null;
        List<String> unprocessedBaseVersions = new ArrayList<>();

        try {
            latestVersion = findLatestVersionInDir(rootSourceDir.toString(), null);
            latestBaseVersion = findLatestVersionInDir(rootBaseSourceDir.toString(), null);
            LOG.info("latestVersion = " + latestVersion + ", latestBaseVersion = " + latestBaseVersion);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }

        if (latestBaseVersion == null) {
            return null;
        }

        if (latestVersion == null || latestBaseVersion.compareTo(latestVersion) > 0) {
            try {
                String pathForSuccessFlagLookup = rootBaseSourceDir + HDFS_PATH_SEPARATOR + latestBaseVersion;
                if (!isAlreadyBeingProcessed(source, latestBaseVersion) && hasSuccessFlag(pathForSuccessFlagLookup)) {
                    unprocessedBaseVersions.add(latestBaseVersion);
                    return unprocessedBaseVersions;
                }
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_25010, e);
            }
        }

        return unprocessedBaseVersions;
    }

    @Override
    String workflowAvroDir(TransformationProgress progress) {
        return workflowDirInHdfs(progress) + HDFS_PATH_SEPARATOR + AVRO_DIR_FOR_CONVERSION;
    }
}
