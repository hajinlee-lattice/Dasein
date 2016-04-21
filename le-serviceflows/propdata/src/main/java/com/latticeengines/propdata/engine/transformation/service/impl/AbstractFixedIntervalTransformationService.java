package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.source.FixedIntervalSource;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public abstract class AbstractFixedIntervalTransformationService extends AbstractTransformationService {

    private static Logger LOG = LogManager.getLogger(AbstractFixedIntervalTransformationService.class);

    protected TransformationProgress transformHook(TransformationProgress progress) {
        if (!transformDataAndUpdateProgress(progress)) {
            return progress;
        }
        return null;
    }

    private boolean transformDataAndUpdateProgress(TransformationProgress progress) {
        String workflowDir = workflowDirInHdfs(progress);
        if (!cleanupHdfsDir(workflowDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + workflowDir, null);
            return false;
        }
        try {
            executeDataFlow(progress, workflowDir);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to transform data.", e);
            return false;
        }

        return doPostProcessing(progress, workflowDir);
    }

    @Override
    protected String getRootBaseSourceDirPath() {
        FixedIntervalSource source = (FixedIntervalSource) getSource();
        return getHdfsPathBuilder().constructSourceDir(source.getBaseSources()[0]).toString();
    }

    @Override
    public String findUnprocessedVersion() {
        Source source = getSource();
        String rootSourceDir = sourceDirInHdfs(source);
        String rootBaseSourceDir = getRootBaseSourceDirPath();
        String rootDirForVersionLookup = rootBaseSourceDir + HDFS_PATH_SEPARATOR
                + ((FixedIntervalSource) source).getDirForBaseVersionLookup();
        List<String> latestVersions = null;
        List<String> latestBaseVersions = null;
        try {
            latestVersions = findSortedVersionsInDir(rootSourceDir);
            latestBaseVersions = findSortedVersionsInDir(rootDirForVersionLookup);

            if (latestBaseVersions.isEmpty()) {
                LOG.info("No version if found in base source");
                return null;
            }

            String versionToProcess = compareVersionLists(latestBaseVersions, latestVersions);
            if (versionToProcess == null) {
                LOG.info("Didn't find any unprocessed version in base source");
                return null;
            }
            return versionToProcess;
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
    }

    @Override
    public TransformationConfiguration createTransformationConfiguration(String versionToProcess) {
        TransformationConfiguration configuration;
        configuration = createNewConfiguration(versionToProcess, versionToProcess);
        return configuration;
    }

    /*
     * GOAL: Ensure that over the period of time missing versions and also
     * handled.
     * 
     * LOGIC: return the first element (in high-to-low order) from
     * latestBaseVersions for which there is no entry in latestVersions list
     */
    private String compareVersionLists(List<String> latestBaseVersions, List<String> latestVersions) {
        String unprocessedVersion = null;
        if (latestVersions.size() == 0) {
            // if there is no version in source then then pick latest from base
            // source version list
            return (latestBaseVersions.size() == 0) ? null : latestBaseVersions.get(0);
        }

        for (String baseVersion : latestBaseVersions) {
            for (String latestVersion : latestVersions) {
                LOG.info("Compare: " + unprocessedVersion);
                if (baseVersion.compareTo(latestVersion) <= 0) {
                    if (baseVersion.equals(latestVersion)) {
                        // if found equal version then skip further checking for
                        // this version
                        continue;
                    }
                    // if we found a version that is smaller than baseVersion
                    // then we return base version as this is a missing version
                    return baseVersion;
                }
            }
        }

        return unprocessedVersion;
    }
}
