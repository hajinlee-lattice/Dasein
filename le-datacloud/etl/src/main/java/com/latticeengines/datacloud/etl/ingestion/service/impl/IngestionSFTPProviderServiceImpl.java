package com.latticeengines.datacloud.etl.ingestion.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.datacloud.etl.utils.SftpUtils;
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
    private static Logger log = LoggerFactory.getLogger(IngestionSFTPProviderServiceImpl.class);

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

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
        List<String> result = new ArrayList<>();
        List<String> targetFiles = getTargetFiles(ingestion);
        List<String> existingFiles = getExistingFiles(ingestion);
        Set<String> existingFilesSet = new HashSet<>(existingFiles);
        for (String targetFile : targetFiles) {
            if (!existingFilesSet.contains(targetFile)) {
                result.add(targetFile);
                log.info("Found missing file to download: " + targetFile);
            }
        }
        return result;
    }

    private List<String> getTargetFiles(Ingestion ingestion) {
        SftpConfiguration config = (SftpConfiguration) ingestion.getProviderConfiguration();
        String fileNamePattern = config.getFileNamePrefix() + "(.*)" + config.getFileNamePostfix()
                + config.getFileExtension();
        return SftpUtils.getFileNames(config, fileNamePattern);
    }

    private List<String> getExistingFiles(Ingestion ingestion) {
        com.latticeengines.domain.exposed.camille.Path ingestionDir = hdfsPathBuilder
                .constructIngestionDir(ingestion.getIngestionName());
        List<String> result = new ArrayList<String>();
        SftpConfiguration config = (SftpConfiguration) ingestion.getProviderConfiguration();
        final String fileExtension = config.getFileExtension();
        HdfsFileFilter filter = new HdfsFileFilter() {
            @Override
            public boolean accept(FileStatus file) {
                return file.getPath().getName().endsWith(fileExtension);
            }
        };
        try {
            if (HdfsUtils.isDirectory(yarnConfiguration, ingestionDir.toString())) {
                List<String> hdfsFiles = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, ingestionDir.toString(),
                        filter);
                if (!CollectionUtils.isEmpty(hdfsFiles)) {
                    for (String fullName : hdfsFiles) {
                        result.add(new Path(fullName).getName());
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to scan hdfs directory %s", ingestionDir.toString()));
        }
        return result;
    }

    @SuppressWarnings("static-access")
    private void checkCompleteVersionFromSftp(IngestionProgress progress) {
        SftpConfiguration sftpConfig = (SftpConfiguration) progress.getIngestion().getProviderConfiguration();
        com.latticeengines.domain.exposed.camille.Path hdfsDir = hdfsPathBuilder
                .constructIngestionDir(progress.getIngestion().getIngestionName(), progress.getVersion());
        Path success = new Path(hdfsDir.toString(), hdfsPathBuilder.SUCCESS_FILE);
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, success.toString())) {
                return;
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to check %s in HDFS dir %s", hdfsPathBuilder.SUCCESS_FILE,
                    hdfsDir.toString()), e);
        }

        String fileNamePattern = ingestionVersionService.getFileNamePattern(progress.getVersion(),
                sftpConfig.getFileNamePrefix(),
                sftpConfig.getFileNamePostfix(), sftpConfig.getFileExtension(), sftpConfig.getFileTimestamp());
        List<String> sftpFiles = SftpUtils.getFileNames(sftpConfig, fileNamePattern);

        List<String> hdfsFiles = getHdfsFileNamesByExtension(hdfsDir.toString(), sftpConfig.getFileExtension());
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


}
