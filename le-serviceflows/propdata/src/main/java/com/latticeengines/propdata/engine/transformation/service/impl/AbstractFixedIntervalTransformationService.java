package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.util.List;

import org.springframework.util.CollectionUtils;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.source.FixedIntervalSource;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public abstract class AbstractFixedIntervalTransformationService extends AbstractTransformationService {
    protected TransformationProgress transformHook(TransformationProgress progress) {
        if (!transformDataAndUpdateProgress(progress)) {
            return progress;
        }
        return null;
    }

    private boolean transformDataAndUpdateProgress(TransformationProgress progress) {
        String targetDir = workflowDirInHdfs(progress);
        if (!cleanupHdfsDir(targetDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + targetDir, null);
            return false;
        }
        try {
            executeDataFlow(progress, targetDir);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to transform data.", e);
            return false;
        }
        return true;
    }

    @Override
    protected String getRootBaseSourceDirPath() {
        FixedIntervalSource source = (FixedIntervalSource) getSource();
        return getHdfsPathBuilder().constructSourceDir(source.getBaseSources()[0]).toString();
    }

    @Override
    public TransformationConfiguration createTransformationConfiguration() {
        Source source = getSource();
        String rootSourceDir = sourceDirInHdfs(source);
        String rootBaseSourceDir = getRootBaseSourceDirPath();
        String latestVersion = null;
        List<String> latestVersions = null;
        String newLatestVersion = null;
        String latestBaseVersion = null;
        List<String> latestBaseVersions = null;

        try {
            // in next txn, fix this logic to support any missing version
            // as well. for now it is latest version comparison only
            latestVersions = findSortedVersionsInDir(rootSourceDir.toString());
            if (CollectionUtils.isEmpty(latestVersions)) {
                latestVersion = latestVersions.get(latestVersions.size() - 1);
            }

            latestBaseVersions = findSortedVersionsInDir(rootBaseSourceDir.toString());

            if (CollectionUtils.isEmpty(latestBaseVersions)) {
                latestBaseVersion = latestBaseVersions.get(latestBaseVersions.size() - 1);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }

        if (latestBaseVersion == null) {
            return null;
        }

        if (latestVersion == null) {
            newLatestVersion = latestBaseVersion;
        } else {
            if (latestBaseVersion.compareTo(latestVersion) > 0) {
                newLatestVersion = latestBaseVersion;

                TransformationConfiguration configuration = createNewConfiguration(latestBaseVersion, newLatestVersion);

                return configuration;
            }
        }

        return null;
    }
}
