package com.latticeengines.propdata.engine.transformation.service.impl;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.FixedIntervalSource;
import com.latticeengines.propdata.core.source.Source;

public abstract class AbstractFixedIntervalTransformationService extends AbstractTransformationService {
    private static Logger LOG = LogManager.getLogger(AbstractFixedIntervalTransformationService.class);
    private static final int SECONDS_TO_MILLIS = 1000;

    abstract List<String> compareVersionLists(Source source, List<String> latestBaseVersions,
            List<String> latestVersions, String baseDir);

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
    public List<String> findUnprocessedVersions() {
        Source source = getSource();
        String rootSourceDir = sourceDirInHdfs(source);
        String rootBaseSourceDir = getRootBaseSourceDirPath();
        String rootDirForVersionLookup = rootBaseSourceDir + HDFS_PATH_SEPARATOR
                + ((FixedIntervalSource) source).getDirForBaseVersionLookup();
        Date cutoffDate = getCutoffDate(null);
        String cutoffDateVersion = HdfsPathBuilder.dateFormat.format(cutoffDate);
        
        List<String> latestVersions = null;
        List<String> latestBaseVersions = null;
        try {
            latestVersions = findSortedVersionsInDir(rootSourceDir, cutoffDateVersion);
            latestBaseVersions = findSortedVersionsInDir(rootDirForVersionLookup, cutoffDateVersion);

            if (latestBaseVersions.isEmpty()) {
                LOG.info("No version if found in base source");
                return null;
            }

            List<String> versionsToProcess = compareVersionLists(source, latestBaseVersions, latestVersions,
                    rootDirForVersionLookup);
            if (versionsToProcess == null) {
                LOG.info("Didn't find any unprocessed version in base source");
                return null;
            }
            return versionsToProcess;
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
    }

    protected Date getCutoffDate(Long overridingCutoffLimitInSeconds) {
        Long currentTimeInMillis = System.currentTimeMillis();
        Long cutoffLimit = ((FixedIntervalSource) getSource()).getCutoffDuration();
        if (overridingCutoffLimitInSeconds != null && overridingCutoffLimitInSeconds > 0) {
            cutoffLimit = overridingCutoffLimitInSeconds;
        }

        return new Date(currentTimeInMillis - cutoffLimit * SECONDS_TO_MILLIS);
    }

    protected boolean shouldSkipVersion(Source source, String baseVersion, String pathForSuccessFlagLookup) {
        boolean shouldSkip = false;
        try {
            if (isUnsafeToProcess(source, baseVersion, pathForSuccessFlagLookup)) {
                shouldSkip = true;
            }
        } catch (IOException e) {
            LOG.error("Could not lookup for success flag" + e);
            shouldSkip = true;
        }

        LOG.info("Should skip version " + baseVersion + " = " + shouldSkip);
        return shouldSkip;
    }

    private boolean isUnsafeToProcess(Source source, String baseVersion, String pathForSuccessFlagLookup)
            throws IOException {
        boolean isUnsafe = isAlreadyBeingProcessed(source, baseVersion) || !hasSuccessFlag(pathForSuccessFlagLookup);
        if (isUnsafe) {
            LOG.info("Unsafe to process base version " + baseVersion);
        }
        return isUnsafe;

    }
}
