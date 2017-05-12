package com.latticeengines.datacloudapi.engine.ingestion.service.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.service.PropDataTenantService;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.datacloud.etl.SftpUtils;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionApiProviderService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionNewProgressValidator;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionProgressService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.datacloudapi.engine.ingestion.service.IngestionService;
import com.latticeengines.domain.exposed.datacloud.ingestion.ApiConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.ingestion.ProviderConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion.IngestionType;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("ingestionService")
public class IngestionServiceImpl implements IngestionService {
    private static Log log = LogFactory.getLog(IngestionServiceImpl.class);

    @Autowired
    IngestionEntityMgr ingestionEntityMgr;

    @Autowired
    private IngestionProgressService ingestionProgressService;

    @Autowired
    private IngestionNewProgressValidator ingestionNewProgressValidator;

    @Autowired
    private IngestionVersionService ingestionVersionService;

    @Autowired
    private PropDataTenantService propDataTenantService;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private IngestionApiProviderService apiProviderService;

    @Autowired
    private EmailService emailService;

    @Value("${propdata.ingestion.notify.email.enabled}")
    private boolean notifyEmailEnabled;

    @Value("${propdata.ingestion.notify.email}")
    private String notifyEmail;

    @Override
    public List<IngestionProgress> scan(String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        killFailedProgresses();
        checkCompleteIngestions();
        ingestAll();
        return kickoffAll();
    }

    @Override
    public IngestionProgress ingestInternal(String ingestionName, IngestionRequest ingestionRequest,
            String hdfsPod) {
        if (StringUtils.isNotEmpty(hdfsPod)) {
            HdfsPodContext.changeHdfsPodId(hdfsPod);
        }
        Ingestion ingestion = getIngestionByName(ingestionName);
        IngestionProgress progress = ingestionProgressService.createPreprocessProgress(ingestion,
                ingestionRequest.getSubmitter(), ingestionRequest.getFileName());
        if (ingestion.getIngestionType() == IngestionType.SFTP
                && !SftpUtils.ifFileExists((SftpConfiguration) ingestion.getProviderConfiguration(),
                        ingestionRequest.getFileName())) {
            return ingestionProgressService.updateInvalidProgress(progress,
                    ingestionRequest.getFileName() + " does not exist in source SFTP");
        }
        if (ingestionNewProgressValidator.isDuplicateProgress(progress)) {
            return ingestionProgressService.updateInvalidProgress(progress,
                    "There is already a progress ingesting " + progress.getSource() + " to "
                            + progress.getDestination());
        }
        return ingestionProgressService.saveProgress(progress);
    }

    @Override
    public Ingestion getIngestionByName(String ingestionName) {
        Ingestion ingestion = ingestionEntityMgr.getIngestionByName(ingestionName);
        if (ingestion == null) {
            throw new IllegalArgumentException(String.format("Fail to find ingestion %s", ingestionName));
        }
        return ingestion;
    }

    @Override
    public void killFailedProgresses() {
        Map<String, Object> fields = new HashMap<String, Object>();
        fields.put("Status", ProgressStatus.PROCESSING);
        List<IngestionProgress> progresses = ingestionProgressService.getProgressesByField(fields);
        for (IngestionProgress progress : progresses) {
            ApplicationId appId = ConverterUtils.toApplicationId(progress.getApplicationId());
            try {
                ApplicationReport report = YarnUtils.getApplicationReport(yarnConfiguration, appId);
                if (report == null || report.getYarnApplicationState() == null
                        || report.getYarnApplicationState().equals(YarnApplicationState.FAILED)
                        || report.getYarnApplicationState().equals(YarnApplicationState.KILLED)) {
                    progress = ingestionProgressService.updateProgress(progress)
                            .status(ProgressStatus.FAILED)
                            .errorMessage(
                                    "Found application status to be FAILED or KILLED in the scan")
                            .commit(true);
                    log.info("Kill progress: " + progress.toString());
                }
            } catch (YarnException | IOException e) {
                log.error("Failed to track application status for " + progress.getApplicationId()
                        + ". Error: " + e.toString());
                if (e.getMessage().contains("doesn't exist in the timeline store")) {
                    progress = ingestionProgressService.updateProgress(progress)
                            .status(ProgressStatus.FAILED)
                            .errorMessage("Failed to track application status in the scan")
                            .commit(true);
                    log.info("Kill progress: " + progress.toString());
                }
            }
        }
    }

    @Override
    public void checkCompleteIngestions() {
        List<Ingestion> ingestions = ingestionEntityMgr.findAll();
        for (Ingestion ingestion : ingestions) {
            List<String> mostRecentVersions = null;
            switch (ingestion.getIngestionType()) {
            case SFTP:
                SftpConfiguration sftpConfig = (SftpConfiguration) ingestion.getProviderConfiguration();
                mostRecentVersions = ingestionVersionService
                        .getMostRecentVersionsFromHdfs(ingestion.getIngestionName(), sftpConfig.getCheckVersion());
                for (String version : mostRecentVersions) {
                    checkCompleteVersionFromSftp(ingestion.getIngestionName(), version, sftpConfig);
                }
                break;
            case API:
                ApiConfiguration apiConfig = (ApiConfiguration) ingestion.getProviderConfiguration();
                mostRecentVersions = ingestionVersionService.getMostRecentVersionsFromHdfs(ingestion.getIngestionName(),
                        apiConfig.getCheckVersion());
                for (String version : mostRecentVersions) {
                    checkCompleteVersionFromApi(ingestion.getIngestionName(), version, apiConfig);
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Ingestion type %s is not supported", ingestion.getIngestionType()));
            }
            ingestionVersionService.updateCurrentVersion(ingestion, mostRecentVersions);
        }
    }

    @SuppressWarnings("static-access")
    private void checkCompleteVersionFromSftp(String ingestionName, String version,
            SftpConfiguration sftpConfig) {
        com.latticeengines.domain.exposed.camille.Path hdfsDir = hdfsPathBuilder.constructIngestionDir(ingestionName,
                version);
        Path success = new Path(hdfsDir.toString(), hdfsPathBuilder.SUCCESS_FILE);
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, success.toString())) {
                return;
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to check %s in HDFS dir %s", hdfsPathBuilder.SUCCESS_FILE,
                    hdfsDir.toString()), e);
        }

        String fileNamePattern = ingestionVersionService.getFileNamePattern(version,
                sftpConfig.getFileNamePrefix(), sftpConfig.getFileNamePostfix(),
                sftpConfig.getFileExtension(), sftpConfig.getFileTimestamp());
        List<String> sftpFiles = SftpUtils.getFileNames(sftpConfig, fileNamePattern);

        List<String> hdfsFiles = getHdfsFileNamesByExtension(hdfsDir.toString(),
                sftpConfig.getFileExtension());
        if (sftpFiles.size() > hdfsFiles.size()) {
            return;
        }
        Set<String> hdfsFileSet = new HashSet<String>(hdfsFiles);
        for (String sftpFile : sftpFiles) {
            if (!hdfsFileSet.contains(sftpFile)) {
                return;
            }
        }

        try {
            HdfsUtils.writeToFile(yarnConfiguration, success.toString(), "");
            if (notifyEmailEnabled) {
                String subject = String.format("Ingestion %s for version %s is finished", ingestionName, version);
                String content = String.format("Files are accessible at the following HDFS folder: %s",
                        hdfsDir.toString());
                emailService.sendSimpleEmail(subject, content, "text/plain", Collections.singleton(notifyEmail));
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to create %s in HDFS dir %s", hdfsPathBuilder.SUCCESS_FILE,
                    hdfsDir.toString()), e);
        }
    }

    @SuppressWarnings("static-access")
    private void checkCompleteVersionFromApi(String ingestionName, String version, ApiConfiguration apiConfig) {
        com.latticeengines.domain.exposed.camille.Path hdfsDir = hdfsPathBuilder.constructIngestionDir(ingestionName,
                version);
        Path success = new Path(hdfsDir.toString(), hdfsPathBuilder.SUCCESS_FILE);
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, success.toString())) {
                return;
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to check %s in HDFS dir %s", hdfsPathBuilder.SUCCESS_FILE,
                    hdfsDir.toString()), e);
        }
        Path file = new Path(hdfsDir.toString(), apiConfig.getFileName());
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, file.toString())) {
                HdfsUtils.writeToFile(yarnConfiguration, success.toString(), "");
                if (notifyEmailEnabled) {
                    String subject = String.format("Ingestion %s for version %s is finished", ingestionName, version);
                    String content = String.format("Files are accessible at the following HDFS folder: %s",
                            hdfsDir.toString());
                    emailService.sendSimpleEmail(subject, content, "text/plain", Collections.singleton(notifyEmail));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to check %s in HDFS or create %s in HDFS dir %s",
                    file.toString(), hdfsPathBuilder.SUCCESS_FILE, hdfsDir.toString()), e);
        }
    }

    private List<String> getHdfsFileNamesByExtension(String hdfsDir, final String fileExtension) {
        HdfsFilenameFilter filter = new HdfsFilenameFilter() {
            @Override
            public boolean accept(String filename) {
                return filename.endsWith(fileExtension);
            }
        };
        List<String> result = new ArrayList<String>();
        try {
            if (HdfsUtils.isDirectory(yarnConfiguration, hdfsDir.toString())) {
                List<String> hdfsFiles = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsDir,
                        filter);
                if (!CollectionUtils.isEmpty(hdfsFiles)) {
                    for (String fullName : hdfsFiles) {
                        result.add(new Path(fullName).getName());
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Failed to scan hdfs directory %s", hdfsDir.toString()), e);
        }
        return result;
    }

    private void ingestAll() {
        List<Ingestion> ingestions = ingestionEntityMgr.findAll();
        List<IngestionProgress> progresses = new ArrayList<IngestionProgress>();
        for (Ingestion ingestion : ingestions) {
            if (ingestionNewProgressValidator.isIngestionTriggered(ingestion)) {
                log.info("Triggered Ingestion: " + ingestion.toString());
                progresses.addAll(getPreprocessProgresses(ingestion));
            }
        }
        progresses = ingestionNewProgressValidator.checkDuplicateProgresses(progresses);
        ingestionProgressService.saveProgresses(progresses);
    }

    private List<IngestionProgress> getPreprocessProgresses(Ingestion ingestion) {
        List<IngestionProgress> progresses = new ArrayList<IngestionProgress>();
        switch (ingestion.getIngestionCriteria()) {
            case ANY_MISSING_FILE:
                List<String> missingFiles = getMissingFiles(ingestion);
                for (String file : missingFiles) {
                    IngestionProgress progress = ingestionProgressService.createPreprocessProgress(
                            ingestion, PropDataConstants.SCAN_SUBMITTER, file);
                    progresses.add(progress);
                }
                break;
            case ALL_DATA:
                SimpleDateFormat format = new SimpleDateFormat("_yyyy_MM");
                String fileName = format.format(new Date());
                IngestionProgress progress = ingestionProgressService.createPreprocessProgress(
                        ingestion, PropDataConstants.SCAN_SUBMITTER, fileName);
                    progresses.add(progress);
                break;
            default:
            throw new UnsupportedOperationException(
                    String.format("Ingestion criteria %s is not supported.", ingestion.getIngestionCriteria()));
        }
        return progresses;
    }

    @Override
    public List<String> getMissingFiles(Ingestion ingestion) {
        List<String> result = new ArrayList<String>();
        switch (ingestion.getIngestionType()) {
        case SFTP:
            List<String> targetFiles = getTargetFiles(ingestion);
            List<String> existingFiles = getExistingFiles(ingestion);
            Set<String> existingFilesSet = new HashSet<String>(existingFiles);
            for (String targetFile : targetFiles) {
                if (!existingFilesSet.contains(targetFile)) {
                    result.add(targetFile);
                    log.info("Found missing file to download: " + targetFile);
                }
            }
            break;
        case API:
            ApiConfiguration apiConfiguration = (ApiConfiguration) ingestion.getProviderConfiguration();
            String targetVersion = apiProviderService.getTargetVersion(apiConfiguration);
            List<String> existingVersions = ingestionVersionService
                    .getMostRecentVersionsFromHdfs(ingestion.getIngestionName(), 1);
            Set<String> existingVersionsSet = new HashSet<String>(existingVersions);
            if (!existingVersionsSet.contains(targetVersion)) {
                result.add(apiConfiguration.getFileName());
            }
            break;
        default:
            throw new UnsupportedOperationException(
                    String.format("Ingestion type %s is not supported", ingestion.getIngestionType()));
        }

        return result;
    }

    @Override
    public List<String> getTargetFiles(Ingestion ingestion) {
        switch (ingestion.getIngestionType()) {
            case SFTP:
                SftpConfiguration config = (SftpConfiguration) ingestion.getProviderConfiguration();
                String fileNamePattern = config.getFileNamePrefix() + "(.*)"
                        + config.getFileNamePostfix() + config.getFileExtension();
                List<String> fileNames = SftpUtils.getFileNames(config, fileNamePattern);
                return ingestionVersionService.getFileNamesOfMostRecentVersions(fileNames,
                        config.getCheckVersion(), config.getCheckStrategy(),
                        config.getFileTimestamp());
            default:
            throw new UnsupportedOperationException(
                    String.format("Ingestion type %s is not supported", ingestion.getIngestionType()));
        }
    }

    @Override
    public List<String> getExistingFiles(Ingestion ingestion) {
        com.latticeengines.domain.exposed.camille.Path ingestionDir = hdfsPathBuilder
                .constructIngestionDir(ingestion.getIngestionName());
        List<String> result = new ArrayList<String>();
        switch (ingestion.getIngestionType()) {
        case SFTP:
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
                    List<String> hdfsFiles = HdfsUtils.getFilesForDirRecursive(yarnConfiguration,
                            ingestionDir.toString(), filter);
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
        default:
            throw new UnsupportedOperationException(
                    String.format("Ingestion type %s is not supported", ingestion.getIngestionType()));
        }
    }

    private List<IngestionProgress> kickoffAll() {
        List<IngestionProgress> progresses = new ArrayList<IngestionProgress>();
        List<IngestionProgress> newProgresses = ingestionProgressService
                .getNewIngestionProgresses();
        List<IngestionProgress> retryFailedProgresses = ingestionProgressService
                .getRetryFailedProgresses();
        List<IngestionProgress> processingProgresses = ingestionProgressService
                .getProcessingProgresses();
        Map<String, Integer> processingIngestion = new HashMap<String, Integer>();
        for (IngestionProgress progress : processingProgresses) {
            if (!processingIngestion.containsKey(progress.getIngestionName())) {
                processingIngestion.put(progress.getIngestionName(), 1);
            } else {
                processingIngestion.put(progress.getIngestionName(),
                        processingIngestion.get(progress.getIngestionName()) + 1);
            }
        }
        for (IngestionProgress progress : retryFailedProgresses) {
            if (!processingIngestion.containsKey(progress.getIngestionName())) {
                progresses.add(progress);
                processingIngestion.put(progress.getIngestionName(), 1);
            } else if (progress.getIngestion().getProviderConfiguration().getConcurrentNum() > processingIngestion
                    .get(progress.getIngestionName())) {
                progresses.add(progress);
                processingIngestion.put(progress.getIngestionName(),
                        processingIngestion.get(progress.getIngestionName()) + 1);
            }
        }
        for (IngestionProgress progress : newProgresses) {
            if (!processingIngestion.containsKey(progress.getIngestionName())) {
                progresses.add(progress);
                processingIngestion.put(progress.getIngestionName(), 1);
            } else if (progress.getIngestion().getProviderConfiguration().getConcurrentNum() > processingIngestion
                    .get(progress.getIngestionName())) {
                progresses.add(progress);
                processingIngestion.put(progress.getIngestionName(),
                        processingIngestion.get(progress.getIngestionName()) + 1);
            }
        }
        return submitAll(progresses);
    }

    private List<IngestionProgress> submitAll(List<IngestionProgress> progresses) {
        List<IngestionProgress> submittedProgresses = new ArrayList<IngestionProgress>();
        Boolean serviceTenantBootstrapped = false;
        for (IngestionProgress progress : progresses) {
            try {
                if (!serviceTenantBootstrapped) {
                    propDataTenantService.bootstrapServiceTenant();
                    serviceTenantBootstrapped = true;
                }
                ApplicationId applicationId = submitWorkflow(progress);
                progress = ingestionProgressService.updateSubmittedProgress(progress,
                        applicationId.toString());
                submittedProgresses.add(progress);
                log.info("Submitted workflow for progress [" + progress + "]. ApplicationID = "
                        + applicationId.toString());
            } catch (Exception e) {
                log.error("Failed to submit workflow for progress " + progress, e);
            }
        }
        return submittedProgresses;
    }

    private ApplicationId submitWorkflow(IngestionProgress progress) {
        Ingestion ingestion = progress.getIngestion();
        ProviderConfiguration providerConfiguration = ingestion.getProviderConfiguration();
        return new IngestionWorkflowSubmitter() //
                .workflowProxy(workflowProxy) //
                .ingestionProgress(progress) //
                .ingestion(ingestion) //
                .providerConfiguration(providerConfiguration) //
                .submit();
    }


}
