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
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

public abstract class AbstractFirehoseTransformationService extends AbstractTransformationService {
    private static Logger LOG = LogManager.getLogger(AbstractFirehoseTransformationService.class);

    private static final String AVRO_DIR_FOR_CONVERSION = "AVRO_DIR_FOR_CONVERSION";

    abstract void uploadSourceSchema(String workflowDir) throws IOException;

    @Override
    protected TransformationProgress transformHook(TransformationProgress progress,
            TransformationConfiguration transformationConfiguration) {
        if (!ingestDataFromFirehoseAndUpdateProgress(progress, transformationConfiguration)) {
            return progress;
        }
        return null;
    }

    private boolean ingestDataFromFirehoseAndUpdateProgress(TransformationProgress progress,
            TransformationConfiguration transformationConfiguration) {
        String workflowDir = workflowDirInHdfs(progress);
        if (!cleanupHdfsDir(workflowDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + workflowDir, null);
            return false;
        }

        try {
            uploadSourceSchema(workflowDir);
            executeDataFlow(progress, workflowDir, transformationConfiguration);
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

        List<String> latestVersions = null;
        List<String> latestBaseVersions = null;
        try {
            latestVersions = findSortedVersionsInDir(rootSourceDir, null);
            latestBaseVersions = findSortedVersionsInDir(rootBaseSourceDir, null);

            if (latestBaseVersions.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No version if found in base source");
                }
                return null;
            }

            List<String> versionsToProcess = compareVersionLists(source, latestBaseVersions, latestVersions,
                    rootBaseSourceDir);
            if (versionsToProcess == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Didn't find any unprocessed version in base source");
                }
                return null;
            }
            return versionsToProcess;
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
    }

    /*
     * GOAL: Ensure that over the period of time missing versions and also
     * handled.
     * 
     * LOGIC: return the first element (in high-to-low order) from
     * latestBaseVersions for which there is no entry in latestVersions list
     */
    protected List<String> compareVersionLists(Source source, List<String> latestBaseVersions,
            List<String> latestVersions, String baseDir) {
        List<String> unprocessedBaseVersion = new ArrayList<>();

        for (String baseVersion : latestBaseVersions) {
            String pathForSuccessFlagLookup = baseDir + HDFS_PATH_SEPARATOR + baseVersion;
            boolean shouldSkip = false;
            if (latestVersions.size() == 0) {
                // if there is no version in source then then pick most recent
                // unprocessed version from base source version list

                shouldSkip = shouldSkipVersion(source, baseVersion, pathForSuccessFlagLookup);

                if (shouldSkip) {
                    continue;
                }

                unprocessedBaseVersion.add(baseVersion);
                return unprocessedBaseVersion;
            }

            boolean foundProcessedEntry = false;
            // try to find matching version in source version list
            for (String latestVersion : latestVersions) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Compare: " + baseVersion);
                }
                if (baseVersion.equals(latestVersion)) {
                    // if found equal version then skip further checking for
                    // this version and break from this inner loop
                    foundProcessedEntry = true;
                    break;
                } else {
                    if (baseVersion.compareTo(latestVersion) > 0) {
                        // if here, that means no corresponding version (equal)
                        // is found in source version list till now and in
                        // sorted order as soon as we see smaller version from
                        // base version than just break loop if any smaller
                        // version is seen
                        break;
                    }
                    continue;
                }
            }

            if (!foundProcessedEntry) {
                // if no equal version found in source version list then we
                // should process this base version as long as it is safe to
                // process it. Otherwise loop over to next base version
                shouldSkip = shouldSkipVersion(source, baseVersion, pathForSuccessFlagLookup);
                if (shouldSkip) {
                    continue;
                }

                // if we found a version that is smaller than baseVersion
                // then we return base version as this is a missing version
                unprocessedBaseVersion.add(baseVersion);
                return unprocessedBaseVersion;
            }
        }
        return unprocessedBaseVersion;
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

        if (LOG.isDebugEnabled()) {
            LOG.debug("Should skip version " + baseVersion + " = " + shouldSkip);
        }
        return shouldSkip;
    }

    private boolean isUnsafeToProcess(Source source, String baseVersion, String pathForSuccessFlagLookup)
            throws IOException {
        boolean isUnsafe = isAlreadyBeingProcessed(source, baseVersion) || !hasSuccessFlag(pathForSuccessFlagLookup);
        if (isUnsafe) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unsafe to process base version " + baseVersion);
            }
        }
        return isUnsafe;

    }

    @Deprecated
    protected List<String> findUnprocessedLatestVersion() {
        Source source = getSource();
        String rootSourceDir = sourceDirInHdfs(source);
        String rootBaseSourceDir = getRootBaseSourceDirPath();
        String latestVersion = null;
        String latestBaseVersion = null;
        List<String> unprocessedBaseVersions = new ArrayList<>();

        try {
            latestVersion = findLatestVersionInDir(rootSourceDir.toString(), null);
            latestBaseVersion = findLatestVersionInDir(rootBaseSourceDir.toString(), null);
            if (LOG.isDebugEnabled()) {
                LOG.debug("latestVersion = " + latestVersion + ", latestBaseVersion = " + latestBaseVersion);
            }
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
