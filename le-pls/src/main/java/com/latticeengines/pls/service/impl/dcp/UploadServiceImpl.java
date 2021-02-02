package com.latticeengines.pls.service.impl.dcp;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;

import com.google.common.base.Preconditions;
import com.latticeengines.app.exposed.service.FileDownloadService;
import com.latticeengines.app.exposed.service.FileDownloader;
import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.app.exposed.util.FileDownloaderRegistry;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.DownloadFileType;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadConfig;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadEmailInfo;
import com.latticeengines.domain.exposed.dcp.UploadFileDownloadConfig;
import com.latticeengines.domain.exposed.dcp.UploadJobDetails;
import com.latticeengines.domain.exposed.dcp.UploadJobStep;
import com.latticeengines.domain.exposed.serviceflows.dcp.DCPSourceImportWorkflowConfiguration;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.pls.service.dcp.UploadService;
import com.latticeengines.proxy.exposed.dcp.SourceProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;

@Service
public class UploadServiceImpl implements UploadService, FileDownloader<UploadFileDownloadConfig> {

    private static final Logger log = LoggerFactory.getLogger(UploadServiceImpl.class);

    private static final int DEFAULT_PAGE_SIZE = 20;

    @Inject
    private FileDownloadService fileDownloadService;

    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private ImportFromS3Service importFromS3Service;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private EmailService emailService;

    @Inject
    private WorkflowJobService workflowJobService;

    @Inject
    private SourceProxy sourceProxy;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @PostConstruct
    public void postConstruct() {
        FileDownloaderRegistry.register(this);
    }

    @Override
    public List<UploadDetails> getAllBySourceId(String sourceId, Upload.Status status, Boolean includeConfig) {
        return getAllBySourceId(sourceId, status, includeConfig, 1, DEFAULT_PAGE_SIZE);
    }

    @Override
    public List<UploadDetails> getAllBySourceId(String sourceId, Upload.Status status, Boolean includeConfig,
                                                int pageIndex, int pageSize) {
        Preconditions.checkNotNull(MultiTenantContext.getCustomerSpace());
        Preconditions.checkArgument(pageIndex > 0);
        Preconditions.checkArgument(pageSize > 0);
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return uploadProxy.getUploads(customerSpace, sourceId, status, includeConfig, pageIndex - 1, pageSize);
    }

    @Override
    public UploadDetails getByUploadId(String uploadId, Boolean includeConfig) {
        Preconditions.checkNotNull(MultiTenantContext.getCustomerSpace());
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        return uploadProxy.getUploadByUploadId(customerSpace, uploadId, includeConfig);
    }

    @Override
    public Class<UploadFileDownloadConfig> configClz() {
        return UploadFileDownloadConfig.class;
    }

    @Override
    public void downloadByConfig(UploadFileDownloadConfig downloadConfig, HttpServletRequest request,
                                 HttpServletResponse response) throws IOException {
        String tenantId = MultiTenantContext.getShortTenantId();
        String uploadId = downloadConfig.getUploadId();
        UploadDetails upload = uploadProxy.getUploadByUploadId(tenantId, uploadId, Boolean.TRUE);

        Preconditions.checkNotNull(upload, "object shouldn't be null");
        UploadConfig config = upload.getUploadConfig();
        List<String> pathsToDownload = config.getDownloadPaths()
                .stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        Preconditions.checkState(CollectionUtils.isNotEmpty(pathsToDownload),
                String.format("empty settings in upload config for %s", uploadId));

        String fileId = config.getDropFilePath();
        if (fileId == null) {
            fileId = config.getUploadRawFilePath();
        }
        fileId = fileId.substring(fileId.lastIndexOf('/') + 1);

        String displayPrefix = PathUtils.getFileNameWithoutExtension(upload.getDisplayName()); // strip file extension

        response.setHeader("Content-Encoding", "gzip");
        response.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + displayPrefix + "_Results.zip\"");

        // the download part will download files in path in UploadConfig: uploadRawFilePath,
        // uploadImportedFilePath, uploadMatchResultPrefix, uploadImportedErrorFilePath.
        // search csv file under these folders recursively, returned paths are absolute
        // from protocol to file name
        /*
        "upload_config": {
        "upload_ts_prefix": "2020-04-22-18-17-04.697",
        "upload_raw_file_path": "dropfolder/4lqucmbu/Projects/Project_powlhlh9/Source/Source_s2kqvrwd/
            upload/2020-04-22-18-17-04.697/RawFile/Account_1_900.csv",
        "upload_match_result_prefix": "/Projects/Project_powlhlh9/Source/Source_s2kqvrwd/
            upload/2020-04-22-18-17-04.697/MatchResult/",
        "upload_imported_error_file_path": "Projects/Project_powlhlh9/Source/Source_s2kqvrwd/
            upload/2020-04-22-18-17-04.697/ImportResult/ImportError/Account_1_900_error.csv"
        }
         */
        List<String> paths = new ArrayList<>();
        String uploadTS = config.getUploadTimestamp();
        String rawPath = config.getUploadRawFilePath();
        String commonPrefix = rawPath.substring(0, rawPath.indexOf(uploadTS) + uploadTS.length() + 1);
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
        for (String path : pathsToDownload) {
            String specialPart = path.substring(path.indexOf(uploadTS) + uploadTS.length() + 1);
            if (path.endsWith(".csv")) {
                paths.add(pathBuilder.getS3BucketDir(s3Bucket) + pathBuilder.getPathSeparator()
                        + commonPrefix + specialPart);
            } else {
                path = commonPrefix + specialPart;
                final String filter = ".*.csv";
                List<String> filePaths = importFromS3Service.getFilesForDir(path,
                        filename -> {
                            String name = FilenameUtils.getName(filename);
                            return name.matches(filter);
                        });
                paths.addAll(filePaths);
            }
        }
        log.info(paths.toString());

        Map<String, DownloadFileType> fileToType = new HashMap<>();
        for (String path : paths) {
            fileToType.put(path, determineFileType(fileId, path, config));
        }

        paths = paths.stream().filter(path -> includeInDownload(downloadConfig, fileToType.get(path))).collect(Collectors.toList());

        Preconditions.checkState(CollectionUtils.isNotEmpty(paths),
                String.format("no files to download for %s", uploadId));

        log.info("download files: " + paths);
        ZipOutputStream zipOut = new ZipOutputStream(new GzipCompressorOutputStream(response.getOutputStream()));
        List<String> names = new ArrayList<>();
        for (String filePath : paths) {
            Path path = new Path(filePath);
            FileSystem system = path.getFileSystem(yarnConfiguration);
            InputStream in = system.open(path);
            String fileName = generateFileName(displayPrefix, filePath, fileToType.get(filePath));
            ZipEntry zipEntry = new ZipEntry(fileName);
            names.add(fileName);
            zipOut.putNextEntry(zipEntry);
            try {
                IOUtils.copyLarge(in, zipOut);
            } catch (Exception e) {
                log.info("unexpected error when copying file: {}", e.getMessage());
            }
            in.close();
            zipOut.closeEntry();
        }
        log.info(names.toString());
        zipOut.finish();
        zipOut.close();
    }

    private DownloadFileType determineFileType(String fileId, String filePath, UploadConfig config) {
        if (filePath.endsWith(config.getUploadRawFilePath())) {
            return DownloadFileType.RAW;
        }
        if (config.getUploadImportedErrorFilePath() != null && filePath.endsWith(config.getUploadImportedErrorFilePath())) {
            return DownloadFileType.IMPORT_ERRORS;
        }
        if (filePath.endsWith(config.getUploadMatchResultAccepted())) {
            return DownloadFileType.MATCHED;
        }
        if (filePath.endsWith(config.getUploadMatchResultRejected())) {
            return DownloadFileType.UNMATCHED;
        }
        if (filePath.endsWith(config.getUploadMatchResultErrored())) {
            return DownloadFileType.PROCESS_ERRORS;
        }

        // unknown/unexpected file
        return null;
    }

    private boolean includeInDownload(UploadFileDownloadConfig config, DownloadFileType fileType) {
        if (fileType == null) {
            return false;
        }
        switch (fileType) {
            case RAW:
                return config.getIncludeRaw();
            case MATCHED:
                return config.getIncludeMatched();
            case UNMATCHED:
                return config.getIncludeUnmatched();
            case IMPORT_ERRORS:
                return config.getIncludeIngestionErrors();
            case PROCESS_ERRORS:
                return config.getIncludeProcessingErrors();

            default:
                return false;
        }
    }

    private String generateFileName(String displayPrefix, String filePath, DownloadFileType fileType) {
        if (fileType != null) {
            switch (fileType) {
                case RAW:
                    return displayPrefix + filePath.substring(filePath.lastIndexOf('.'));
                case MATCHED:
                    return displayPrefix + "_Matched.csv";
                case UNMATCHED:
                    return displayPrefix + "_Unmatched.csv";
                case IMPORT_ERRORS:
                    return displayPrefix + "_Ingestion_Errors.csv";
                case PROCESS_ERRORS:
                    return displayPrefix + "_Processing_Errors.csv";

                default:
                    break;
            }
        }
        return filePath.substring(filePath.lastIndexOf("/") + 1);
    }

    @Override
    public String generateToken(String uploadId, List<DownloadFileType> files) {
        UploadFileDownloadConfig config = new UploadFileDownloadConfig();
        config.setUploadId(uploadId);
        boolean includeAll = (files == null || files.isEmpty());
        config.setIncludeRaw(includeAll);
        config.setIncludeMatched(includeAll);
        config.setIncludeUnmatched(includeAll);
        config.setIncludeIngestionErrors(includeAll);
        config.setIncludeProcessingErrors(includeAll);
        if (!includeAll) {
            for (DownloadFileType type : files) {
                switch (type) {
                    case RAW:
                        config.setIncludeRaw(true);
                        break;

                    case MATCHED:
                        config.setIncludeMatched(true);
                        break;

                    case UNMATCHED:
                        config.setIncludeUnmatched(true);
                        break;

                    case IMPORT_ERRORS:
                        config.setIncludeIngestionErrors(true);
                        break;

                    case PROCESS_ERRORS:
                        config.setIncludeProcessingErrors(true);
                        break;

                    default:
                        break;
                }
            }
        }
        return fileDownloadService.generateDownloadToken(config);
    }

    @Override
    public void sendUploadEmail(UploadEmailInfo uploadEmailInfo) {
        switch (uploadEmailInfo.getJobStatus()) {
            case "COMPLETED":
                emailService.sendUploadCompletedEmail(uploadEmailInfo);
                break;
            case "FAILED":
                emailService.sendUploadFailedEmail(uploadEmailInfo);
                break;
            default:
                log.warn("Upload email info job status of " + uploadEmailInfo.getJobStatus() + " is an " +
                        "unknown type and not supported.  No upload status email sent!");
        }
    }

    @Override
    public UploadDetails startImport(DCPImportRequest importRequest) {
        Preconditions.checkNotNull(MultiTenantContext.getCustomerSpace());
        ApplicationId appId = submitImportRequest(importRequest);
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        Job job = workflowJobService.findByApplicationId(appId.toString());
        String uploadId = job.getInputs().get(DCPSourceImportWorkflowConfiguration.UPLOAD_ID);
        return uploadProxy.getUploadByUploadId(customerSpace, uploadId, Boolean.TRUE);
    }

    @Override
    public ApplicationId submitImportRequest(DCPImportRequest importRequest) {
        return uploadProxy.startImport(MultiTenantContext.getShortTenantId(), importRequest);
    }

    @Override
    public UploadJobDetails getJobDetailsByUploadId(String uploadId) {
        UploadJobDetails uploadJobDetails = new UploadJobDetails();
        uploadJobDetails.setUploadId(uploadId);
        UploadDetails uploadDetails = getByUploadId(uploadId, false);
        uploadJobDetails.setDisplayName(uploadDetails.getDisplayName());
        String customerSpace = MultiTenantContext.getCustomerSpace().toString();
        Source source = sourceProxy.getSource(customerSpace, uploadDetails.getSourceId());
        uploadJobDetails.setSourceDisplayName(source.getSourceDisplayName());
        uploadJobDetails.setStatus(uploadDetails.getStatus());
        uploadJobDetails.setStatistics(uploadDetails.getStatistics());
        uploadJobDetails.setUploadDiagnostics(uploadDetails.getUploadDiagnostics());
        Job job = workflowJobService.findByApplicationId(uploadDetails.getUploadDiagnostics().getApplicationId());
        List<UploadJobStep> uploadJobSteps = new ArrayList<>();
        if (job != null && job.getSteps() != null && !job.getSteps().isEmpty()) {
            job.getSteps().stream().forEach(jobStep -> {
                UploadJobStep uploadJobStep = new UploadJobStep();
                uploadJobStep.setStepName(jobStep.getName());
                uploadJobStep.setStepDescription(jobStep.getDescription());
                if(jobStep.getStartTimestamp() != null) {
                    uploadJobStep.setStartTimestamp(jobStep.getStartTimestamp().getTime());
                }
                if(jobStep.getEndTimestamp() != null) {
                    uploadJobStep.setEndTimestamp(jobStep.getEndTimestamp().getTime());
                }
                uploadJobSteps.add(uploadJobStep);
            });
        }
        List<UploadJobStep>mergedJobSteps = mergeDupSteps(uploadJobSteps);
        uploadJobDetails.setUploadJobSteps(mergedJobSteps);
        uploadJobDetails.setDropFileTime(uploadDetails.getDropFileTime());
        uploadJobDetails.setUploadCreatedTime(uploadDetails.getUploadCreatedTime());
        uploadJobDetails.setProgressPercentage(uploadDetails.getProgressPercentage());
        if(uploadJobDetails.getProgressPercentage() < 100 && mergedJobSteps.size() > 0){
            uploadJobDetails.setCurrentStep(mergedJobSteps.get(mergedJobSteps.size() - 1));
        }
        return uploadJobDetails;
    }

    private List<UploadJobStep> mergeDupSteps(List<UploadJobStep> uploadJobSteps) {
        List<UploadJobStep> mergedSteps = new ArrayList<>();
        if (CollectionUtils.isEmpty(uploadJobSteps)) {
            return mergedSteps;
        }
        Map<String, List<UploadJobStep>> stepMap = uploadJobSteps.stream()
                .filter(step -> StringUtils.isNotEmpty(step.getStepName()))
                .collect(Collectors.groupingBy(UploadJobStep::getStepName));
        Set<String> processedStep = new HashSet<>();
        for (UploadJobStep jobStep : uploadJobSteps) {
            if (!processedStep.contains(jobStep.getStepName())) {
                processedStep.add(jobStep.getStepName());
                List<UploadJobStep> stepList = stepMap.get(jobStep.getStepName());
                UploadJobStep currentStep = stepList.get(0);
                for (int i = 1; i < stepList.size(); i++) {
                    if (stepList.get(i).getStartTimestamp() != null) {
                        currentStep.setStartTimestamp(currentStep.getStartTimestamp() == null ?
                                stepList.get(i).getStartTimestamp() : Math.min(currentStep.getStartTimestamp(), stepList.get(i).getStartTimestamp()));
                    }
                    if (currentStep.getEndTimestamp() != null) {
                        currentStep.setEndTimestamp(stepList.get(i).getEndTimestamp() == null ? null :
                                Math.max(stepList.get(i).getEndTimestamp(), currentStep.getEndTimestamp()));
                    }
                }
                mergedSteps.add(currentStep);
            }
        }
        return mergedSteps;
    }
}
