package com.latticeengines.propdata.engine.ingestion.service.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
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
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.propdata.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.propdata.ingestion.ProviderConfiguration;
import com.latticeengines.domain.exposed.propdata.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion.IngestionType;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.propdata.core.PropDataConstants;
import com.latticeengines.propdata.core.service.PropDataTenantService;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.engine.common.EngineConstants;
import com.latticeengines.propdata.engine.common.SftpUtils;
import com.latticeengines.propdata.engine.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.propdata.engine.ingestion.service.IngestionNewProgressValidator;
import com.latticeengines.propdata.engine.ingestion.service.IngestionProgressService;
import com.latticeengines.propdata.engine.ingestion.service.IngestionService;
import com.latticeengines.propdata.engine.ingestion.service.IngestionVersionService;
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
            throw new IllegalArgumentException(
                    "Ingestion " + ingestionName + " does not have configuration.");
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
            if (ingestion.getProviderConfiguration() instanceof SftpConfiguration) {
                SftpConfiguration sftpConfig = (SftpConfiguration) ingestion
                        .getProviderConfiguration();
                List<String> mostRecentVersions = ingestionVersionService
                        .getMostRecentVersionsFromHdfs(ingestion.getIngestionName(),
                                sftpConfig.getCheckVersion());
                for (String version : mostRecentVersions) {
                    checkCompleteVersionFromSftp(ingestion.getIngestionName(), version, sftpConfig);
                }
            }
        }
    }

    private void checkCompleteVersionFromSftp(String ingestionName, String version,
            SftpConfiguration sftpConfig) {
        String fileNamePattern = ingestionVersionService.getFileNamePattern(version,
                sftpConfig.getFileNamePrefix(), sftpConfig.getFileNamePostfix(),
                sftpConfig.getFileExtension(), sftpConfig.getFileTimestamp());
        List<String> sftpFiles = SftpUtils.getFileNames(sftpConfig, fileNamePattern);
        com.latticeengines.domain.exposed.camille.Path hdfsDir = hdfsPathBuilder
                .constructIngestionDir(ingestionName, version);
        List<String> hdfsFiles = getHdfsFileNames(hdfsDir.toString(),
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
        Path success = new Path(hdfsDir.toString(), EngineConstants.INGESTION_SUCCESS);
        try {
            if (!HdfsUtils.fileExists(yarnConfiguration, success.toString())) {
                HdfsUtils.writeToFile(yarnConfiguration, success.toString(), "");
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to create " + EngineConstants.INGESTION_SUCCESS + " in HDFS");
        }
    }

    private List<String> getHdfsFileNames(String hdfsDir, final String fileExtension) {
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
            throw new RuntimeException("Failed to scan hdfs directory " + hdfsDir.toString());
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
                throw new UnsupportedOperationException("Ingestion criteria "
                        + ingestion.getIngestionCriteria() + " is not supported.");
        }
        return progresses;
    }

    @Override
    public List<String> getMissingFiles(Ingestion ingestion) {
        List<String> targetFiles = getTargetFiles(ingestion);
        List<String> existingFiles = getExistingFiles(ingestion);
        Set<String> existingFilesSet = new HashSet<String>(existingFiles);
        List<String> result = new ArrayList<String>();
        for (String targetFile : targetFiles) {
            if (!existingFilesSet.contains(targetFile)) {
                result.add(targetFile);
                log.info("Found missing file to download: " + targetFile);
            }
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
                throw new RuntimeException(
                        "Ingestion type " + ingestion.getIngestionType() + " is not supported");
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
                        List<String> hdfsFiles = HdfsUtils.getFilesForDirRecursive(
                                yarnConfiguration, ingestionDir.toString(), filter);
                        if (!CollectionUtils.isEmpty(hdfsFiles)) {
                            for (String fullName : hdfsFiles) {
                                result.add(new Path(fullName).getName());
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(
                            "Failed to scan hdfs directory " + ingestionDir.toString());
                }
                return result;
            default:
                throw new RuntimeException(
                        "Ingestion type " + ingestion.getIngestionType() + " is not supported");
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
        Set<String> processingIngestion = new HashSet<String>();
        for (IngestionProgress progress : processingProgresses) {
            processingIngestion.add(progress.getIngestionName());
        }
        for (IngestionProgress progress : retryFailedProgresses) {
            if (!processingIngestion.contains(progress.getIngestionName())) {
                progresses.add(progress);
                processingIngestion.add(progress.getIngestionName());
            }
        }
        for (IngestionProgress progress : newProgresses) {
            if (!processingIngestion.contains(progress.getIngestionName())) {
                progresses.add(progress);
                processingIngestion.add(progress.getIngestionName());
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
