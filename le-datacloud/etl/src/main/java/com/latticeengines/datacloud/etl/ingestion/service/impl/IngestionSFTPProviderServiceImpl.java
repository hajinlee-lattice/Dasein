package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.datacloud.etl.utils.SftpUtils;
import com.latticeengines.datacloud.etl.utils.VersionUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.datacloud.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.yarn.exposed.service.JobService;

@Component("ingestionSFTPProviderService")
public class IngestionSFTPProviderServiceImpl extends IngestionProviderServiceImpl {
    private static final Logger log = LoggerFactory.getLogger(IngestionSFTPProviderServiceImpl.class);

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Inject
    private IngestionProgressService ingestionProgressService;

    @Inject
    private IngestionVersionService ingestionVersionService;

    @Inject
    private EaiProxy eaiProxy;

    @Inject
    protected JobService jobService;

    private static final String TMP_PREFIX = "TMP_";

    private static final Integer WORKFLOW_WAIT_TIME_IN_SECOND = (int) TimeUnit.HOURS.toSeconds(24);

    @Override
    public void ingest(IngestionProgress progress) throws Exception {
        String destFile = progress.getDestination();
        // Multiple jobs could be running at same time to download files to same folder.
        // To avoid violation, download file to unique tmp folder first.
        Path tmpDestDir = new Path(new Path(progress.getDestination()).getParent(),
                TMP_PREFIX + UUID.randomUUID().toString());
        Path tmpDestFile = new Path(tmpDestDir, new Path(progress.getDestination()).getName());
        progress.setDestination(tmpDestFile.toString());
        CamelRouteConfiguration camelRouteConfig = ingestionProgressService.createCamelRouteConfiguration(progress);
        progress.setDestination(destFile);
        AppSubmission submission = eaiProxy.submitEaiJob(camelRouteConfig);
        String eaiAppId = submission.getApplicationIds().get(0);
        log.info("EAI Service ApplicationId: " + eaiAppId);
        JobStatus finalStatus = jobService.waitFinalJobStatus(eaiAppId.toString(), WORKFLOW_WAIT_TIME_IN_SECOND);
        if (finalStatus.getStatus() == FinalApplicationStatus.SUCCEEDED
                && waitForFileToBeIngested(tmpDestFile.toString())) {
            HdfsUtils.moveFile(yarnConfiguration, tmpDestFile.toString(), progress.getDestination());
            Long size = HdfsUtils.getFileSize(yarnConfiguration, progress.getDestination());
            progress = ingestionProgressService.updateProgress(progress).size(size).status(ProgressStatus.FINISHED)
                    .commit(true);
            HdfsUtils.rmdir(yarnConfiguration, tmpDestDir.toString());
            log.info("Ingestion finished. Progress: " + progress.toString());
            checkCompleteVersionFromSftp(progress);
        } else {
            progress = ingestionProgressService.updateProgress(progress).status(ProgressStatus.FAILED).commit(true);
            log.error("Ingestion failed. Progress: " + progress.toString());
        }
    }

    @Override
    public List<String> getMissingFiles(Ingestion ingestion) {
        List<String> targetFiles = getTargetFiles(ingestion);
        List<String> existingFiles = getExistingFiles(ingestion);
        return getFilesDiff(ingestion, targetFiles, existingFiles);
    }

    /**
     * Get file list on target SFTP based on check strategy configured
     *
     * @param ingestion
     * @return if subfolder exists {@link SftpConfiguration#hasSubfolder is
     *         true}, return target files in format "subfolder/filename"; if
     *         not, return in format "filename"
     */
    private List<String> getTargetFiles(Ingestion ingestion) {
        SftpConfiguration config = (SftpConfiguration) ingestion.getProviderConfiguration();
        return SftpUtils.getFileList(config);
    }

    /**
     * Get file list existing on HDFS based on check strategy configured
     *
     * @param ingestion
     * @return if {@link SftpConfiguration#hasSubfolder is true}, return
     *         existing files in format "subfolder/filename"; if not, return in
     *         format "filename"
     */
    @VisibleForTesting
    List<String> getExistingFiles(Ingestion ingestion) {
        SftpConfiguration config = (SftpConfiguration) ingestion.getProviderConfiguration();

        List<String> existingVersions = hdfsSourceEntityMgr
                .getVersions(new IngestionSource(ingestion.getIngestionName()));
        List<String> versionsToCompare = VersionUtils.getMostRecentVersionPaths(existingVersions, config.getCheckVersion(), config.getCheckStrategy(),
                HdfsPathBuilder.DATE_FORMAT_STRING, "(.+)", null);

        List<String> existingFiles = new ArrayList<String>();
        for (String version : versionsToCompare) {
            String hdfsDir = hdfsPathBuilder.constructIngestionDir(ingestion.getIngestionName(), version).toString();
            List<String> hdfsFiles = getHdfsFilesByRegexPattern(hdfsDir, config.getFileRegexPattern(), true);
            if (CollectionUtils.isEmpty(hdfsFiles)) {
                continue;
            }
            for (String fileName : hdfsFiles) {
                if (config.hasSubfolder()) {
                    existingFiles.add((new Path(version, fileName)).toString());
                } else {
                    existingFiles.add(fileName);
                }
            }
        }
        return existingFiles;
    }

    /**
     * Compare target files got from getTargetFiles() and existing files got
     * from getExistingFiles() and return files to ingest
     *
     * @param ingestion
     * @param targetFiles:
     *            files on target SFTP
     * @param existingFiles:
     *            files existed on HDFS
     * @return missing file list in same string format with target file list --
     *         if subfolder exists {@link SftpConfiguration#hasSubfolder is
     *         true}, return target files in format "subfolder/filename"; if
     *         not, return in format "filename"
     */
    @VisibleForTesting
    List<String> getFilesDiff(Ingestion ingestion, List<String> targetFiles, List<String> existingFiles) {
        SftpConfiguration config = (SftpConfiguration) ingestion.getProviderConfiguration();

        if (CollectionUtils.isEmpty(existingFiles)) {
            return targetFiles;
        }

        List<String> filesDiff = new ArrayList<>();
        // If there is no subfolder, both target files and existing files only
        // contain file names, then compare diff directly
        if (!config.hasSubfolder()) {
            Set<String> existingSet = new HashSet<>(existingFiles);
            for (String target : targetFiles) {
                if (!existingSet.contains(target)) {
                    filesDiff.add(target);
                }
            }
            return filesDiff;
        }
        // If subfolder is not timestamp versioned, extract file names from
        // target files and existing files (subfolder is ignored), then compare
        // file name diff
        if (StringUtils.isBlank(config.getSubfolderTSPattern())) {
            Set<String> existingSet = existingFiles.stream().map(filePath -> getFileNameFromPath(filePath))
                    .collect(Collectors.toSet());
            for (String targetPath : targetFiles) {
                if (!existingSet.contains(getFileNameFromPath(targetPath))) {
                    filesDiff.add(targetPath);
                }
            }
            return filesDiff;
        }
        // If subfolder is timestamp versioned, extract version from both target
        // files and existing files, then compare file name diff for each
        // version
        // map of <version -> existing file set>
        Map<String, Set<String>> existingMap = existingFiles.stream()
                .collect(Collectors.groupingBy(
                        (filePath) -> VersionUtils.extractTSVersion(filePath, HdfsPathBuilder.DATE_FORMAT_STRING,
                                "(.+)", config.getSubfolderTSPattern(), HdfsPathBuilder.UTC),
                        Collectors.mapping((filePath) -> getFileNameFromPath(filePath), Collectors.toSet())));
        for (String targetPath : targetFiles) {
            String targetVersion = VersionUtils.extractTSVersion(targetPath, config.getSubfolderTSPattern(), null,
                    null, null);
            if (!existingMap.containsKey(targetVersion)
                    || !existingMap.get(targetVersion).contains(getFileNameFromPath(targetPath))) {
                filesDiff.add(targetPath);
            }
        }
        return filesDiff;
    }

    /**
     * For single version of ingestion with multiple files, there will be
     * multiple jobs. At end of each job, check if all the files on SFTP for
     * this version have all be ingested. If so, mark the version as completed
     * by uploading a _SUCCESS file
     *
     * @param progress:
     *            ingestion progress
     */
    @SuppressWarnings("static-access")
    private void checkCompleteVersionFromSftp(IngestionProgress progress) {
        SftpConfiguration sftpConfig = (SftpConfiguration) progress.getIngestion().getProviderConfiguration();
        String hdfsDir = hdfsPathBuilder
                .constructIngestionDir(progress.getIngestion().getIngestionName(), progress.getVersion()).toString();

        Path success = new Path(hdfsDir, hdfsPathBuilder.SUCCESS_FILE);
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, success.toString())) {
                return;
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to check %s in HDFS dir %s", hdfsPathBuilder.SUCCESS_FILE,
                    hdfsDir), e);
        }

        SftpConfiguration sftpConfigCopy = JsonUtils.deserialize(JsonUtils.serialize(sftpConfig),
                SftpConfiguration.class);
        if (sftpConfig.hasSubfolder()) {
            sftpConfigCopy.setHasSubfolder(false);
            sftpConfigCopy.setSftpDir((new Path(progress.getSource())).getParent().toString());
            sftpConfigCopy.setSubfolderRegexPattern(null);
            sftpConfigCopy.setSubfolderTSPattern(null);
        }
        // target file names without path
        List<String> sftpFiles = SftpUtils.getFileList(sftpConfigCopy);

        List<String> hdfsFiles = getHdfsFilesByRegexPattern(hdfsDir, sftpConfigCopy.getFileRegexPattern(), true);
        if (sftpFiles.size() > hdfsFiles.size()) {
            return;
        }
        Set<String> hdfsFileSet = new HashSet<>(hdfsFiles);
        for (String sftpFile : sftpFiles) {
            if (!hdfsFileSet.contains(sftpFile)) {
                return;
            }
        }

        try {
            HdfsUtils.writeToFile(yarnConfiguration, success.toString(), "");
            Path tmpDestDir = new Path(new Path(progress.getDestination()).getParent(), TMP_PREFIX + "*");
            HdfsUtils.rmdir(yarnConfiguration, tmpDestDir.toString());
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to create %s in HDFS dir %s and delete all tmp dirs",
                    hdfsPathBuilder.SUCCESS_FILE, hdfsDir.toString()), e);
        }
        emailNotify(sftpConfig, progress.getIngestion().getIngestionName(), progress.getVersion(), hdfsDir.toString());
        ingestionVersionService.updateCurrentVersion(progress.getIngestion(), progress.getVersion());
    }

    /**
     * Get all the files under specified hdfs path whose names match provided
     * regex pattern
     *
     * @param hdfsDir:
     *            hdfs path
     * @param regexPattern:
     *            regex pattern for file name matching
     * @param nameOnly:
     *            return file name only or full file path
     * @return
     */
    private List<String> getHdfsFilesByRegexPattern(@NotNull String hdfsDir, @NotNull final String regexPattern,
            boolean nameOnly) {
        Preconditions.checkNotNull(hdfsDir);
        Preconditions.checkNotNull(regexPattern);

        Pattern pattern = Pattern.compile(regexPattern);
        HdfsFileFilter filter = new HdfsFileFilter() {
            @Override
            public boolean accept(FileStatus file) {
                Matcher matcher = pattern.matcher(file.getPath().getName());
                return matcher.find();
            }
        };
        List<String> files = new ArrayList<String>();
        try {
            if (HdfsUtils.isDirectory(yarnConfiguration, hdfsDir.toString())) {
                List<String> hdfsFiles = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsDir, filter);
                if (!CollectionUtils.isEmpty(hdfsFiles)) {
                    for (String fullName : hdfsFiles) {
                        if (nameOnly) {
                            files.add(getFileNameFromPath(fullName));
                        } else {
                            files.add(fullName);
                        }
                    }
                }
            } else {
                throw new RuntimeException(hdfsDir + " is not a directory");
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to scan hdfs directory %s", hdfsDir.toString()), e);
        }
        return files;
    }

    private String getFileNameFromPath(String pathStr) {
        Path path = new Path(pathStr);
        return path.getName();
    }

}
