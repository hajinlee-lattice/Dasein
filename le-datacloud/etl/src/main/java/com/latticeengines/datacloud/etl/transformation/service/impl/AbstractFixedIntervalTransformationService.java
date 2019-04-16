package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.datacloud.core.source.FixedIntervalSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public abstract class AbstractFixedIntervalTransformationService<T extends TransformationConfiguration>
        extends AbstractTransformationService<T> {
    private static Logger LOG = LoggerFactory.getLogger(AbstractFixedIntervalTransformationService.class);
    private static final int SECONDS_TO_MILLIS = 1000;

    abstract List<String> compareVersionLists(Source source, List<String> latestBaseVersions,
                                              List<String> latestVersions, String baseDir);
    abstract void executeDataFlow(TransformationProgress progress, String workflowDir, T transformationConfiguration);

    abstract T createNewConfiguration(List<String> latestBaseVersions, String newLatestVersion,
                                      List<SourceColumn> sourceColumns);

    @Override
    public T createTransformationConfiguration(List<String> baseVersionsToProcess, String targetVersion) {
        if (StringUtils.isEmpty(targetVersion)) {
            targetVersion = baseVersionsToProcess.get(0);
        }
        return createNewConfiguration(baseVersionsToProcess, targetVersion,
                sourceColumnEntityMgr.getSourceColumns(getSource().getSourceName()));
    }

    @Override
    protected TransformationProgress transformHook(TransformationProgress progress, T transformationConfiguration) {
        if (transformDataAndUpdateProgress(progress, transformationConfiguration)) {
            return progress;
        }
        return null;
    }

    private boolean transformDataAndUpdateProgress(TransformationProgress progress, T transformationConfiguration) {
        String workflowDir = initialDataFlowDirInHdfs(progress);
        if (!cleanupHdfsDir(workflowDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + workflowDir, null);
            return false;
        }
        try {
            executeDataFlow(progress, workflowDir, transformationConfiguration);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to transform data.", e);
            return false;
        }

        return doPostProcessing(progress, workflowDir);
    }

    protected List<String> getRootBaseSourceDirPaths() {
        FixedIntervalSource source = (FixedIntervalSource) getSource();
        LOG.info("Source Name: " + source.getSourceName() + " Base Source Name: "
                + source.getBaseSources()[0].getSourceName() + " RootBaseSourceDirPath: "
                + hdfsPathBuilder.constructSourceDir(source.getBaseSources()[0].getSourceName()).toString());
        return Collections
                .singletonList(
                        hdfsPathBuilder.constructSourceDir(source.getBaseSources()[0].getSourceName()).toString());
    }

    @Override
    public List<String> findUnprocessedBaseVersions() {
        Source source = getSource();
        String rootSourceDir = sourceDirInHdfs(source);
        String rootBaseSourceDir = getRootBaseSourceDirPaths().get(0);
        String rootDirForVersionLookup = rootBaseSourceDir + HDFS_PATH_SEPARATOR
                + ((FixedIntervalSource) source).getDirForBaseVersionLookup();
        Date cutoffDate = getCutoffDate(null);
        String cutoffDateVersion = HdfsPathBuilder.dateFormat.format(cutoffDate);
        LOG.info("findUnprocessedBaseVersions() source = " + source);
        LOG.info("findUnprocessedBaseVersions() rootSourceDir = " + rootSourceDir);
        LOG.info("findUnprocessedBaseVersions() rootBaseSourceDir = " + rootBaseSourceDir);
        LOG.info("findUnprocessedBaseVersions() rootDirForVersionLookup = " + rootDirForVersionLookup);
        LOG.info("findUnprocessedBaseVersions() cutoffDateVersion = " + cutoffDateVersion);

        List<String> latestVersions = null;
        List<String> latestBaseVersions = null;
        try {
            latestVersions = findSortedVersionsInDir(rootSourceDir, cutoffDateVersion);
            latestBaseVersions = findSortedVersionsInDir(rootDirForVersionLookup, cutoffDateVersion);

            if (latestBaseVersions.isEmpty()) {
                LOG.info("No version is found in base source");
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
