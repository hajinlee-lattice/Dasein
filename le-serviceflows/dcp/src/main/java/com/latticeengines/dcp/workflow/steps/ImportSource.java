package com.latticeengines.dcp.workflow.steps;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.dcp.DownloadFileType;
import com.latticeengines.domain.exposed.dcp.UploadDetails;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.domain.exposed.util.UploadS3PathBuilderUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("importSource")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ImportSource extends BaseWorkflowStep<ImportSourceStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ImportSource.class);

    private static final String ERROR_FILE = "error.csv";
    private static final String ERROR_SUFFIX = "_error";

    @Inject
    private EaiProxy eaiProxy;

    @Inject
    private EaiJobDetailProxy eaiJobDetailProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Inject
    private UploadProxy uploadProxy;

    @Inject
    private S3Service s3Service;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    @Override
    public void execute() {
        log.info("Start import DCP file");
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        UploadDetails upload = uploadProxy.getUploadByUploadId(customerSpace.toString(), configuration.getUploadId(), Boolean.TRUE);
        if (upload == null || upload.getUploadConfig() == null) {
            throw new RuntimeException("Cannot find upload configuration for import!");
        }
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTaskBySourceId(customerSpace.toString(),
                configuration.getSourceId());
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find template for source " + configuration.getSourceId());
        }
        DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace.toString());
        String eaiAppId = importTable(dataFeedTask, dropBoxSummary, upload);
        updateStats(customerSpace.toString(), eaiAppId, upload, dropBoxSummary);
    }

    private String importTable(DataFeedTask dataFeedTask, DropBoxSummary dropBoxSummary,
                               UploadDetails upload) {
        EaiJobConfiguration importConfig = setupConfiguration(dataFeedTask, dropBoxSummary, upload);
        AppSubmission submission = eaiProxy.submitEaiJob(importConfig);
        String applicationId = submission.getApplicationIds().get(0);
        dataFeedTask.setActiveJob(applicationId);
        dataFeedProxy.updateDataFeedTask(importConfig.getCustomerSpace().toString(), dataFeedTask, true);
        saveOutputValue(WorkflowContextConstants.Outputs.EAI_JOB_APPLICATION_ID, applicationId);
        waitForAppId(applicationId);
        return applicationId;
    }

    private void updateUploadStatistics(EaiImportJobDetail jobDetail) {
        UploadStats.ImportStats importStats = new UploadStats.ImportStats();
        long totalCnt = jobDetail.getTotalRows();
        long errorCnt = jobDetail.getIgnoredRows() == null ? 0 : jobDetail.getIgnoredRows();
        if (totalCnt == errorCnt) {
            log.info("No records can be ingested!");
            throw new LedpException(LedpCode.LEDP_60010);
        }
        importStats.setSubmitted(totalCnt);
        importStats.setSuccessfullyIngested(totalCnt - errorCnt);
        importStats.setFailedIngested(errorCnt);
        UploadStats stats = new UploadStats();
        stats.setImportStats(importStats);
        putObjectInContext(UPLOAD_STATS, stats);
    }

    private String extractResultPath(EaiImportJobDetail jobDetail) {
        List<String> paths = JsonUtils.convertList((List<?>) jobDetail.getDetails().get("ExtractPathList"), String.class);
        if (CollectionUtils.isEmpty(paths) || paths.size() != 1) {
            throw new RuntimeException("Should have exactly one extract path, but found {}" + CollectionUtils.size(paths));
        } else {
            String path = PathUtils.toParquetOrAvroDir(paths.get(0));
            log.info("Extracted eai result path {}", path);
            putStringValueInContext(IMPORT_DATA_LOCATION, path);
            return path;
        }
    }

    private S3FileToHdfsConfiguration setupConfiguration(DataFeedTask dataFeedTask, DropBoxSummary dropBoxSummary,
                                                         UploadDetails upload) {
        S3FileToHdfsConfiguration s3FileToHdfsConfiguration = new S3FileToHdfsConfiguration();
        List<String> identifiers = new ArrayList<>();

        s3FileToHdfsConfiguration.setCustomerSpace(configuration.getCustomerSpace());
        s3FileToHdfsConfiguration.setS3Bucket(dropBoxSummary.getBucket());
        s3FileToHdfsConfiguration.setS3FilePath(upload.getUploadConfig().getUploadRawFilePath());
        s3FileToHdfsConfiguration.setBusinessEntity(BusinessEntity.getByName(dataFeedTask.getEntity()));
        s3FileToHdfsConfiguration.setJobIdentifier(dataFeedTask.getUniqueId());
        s3FileToHdfsConfiguration.setNeedDetailError(Boolean.TRUE);
        SourceImportConfiguration sourceImportConfig = new SourceImportConfiguration();
        sourceImportConfig.setSourceType(SourceType.FILE);
        s3FileToHdfsConfiguration.addSourceConfiguration(sourceImportConfig);

        identifiers.add(dataFeedTask.getUniqueId());
        s3FileToHdfsConfiguration.setProperty(ImportProperty.COLLECTION_IDENTIFIERS, JsonUtils.serialize(identifiers));

        return s3FileToHdfsConfiguration;
    }

    private void updateStats(String customerSpace, String eaiAppId, UploadDetails upload, DropBoxSummary dropBoxSummary) {
        EaiImportJobDetail eaiImportJobDetail = eaiJobDetailProxy.getImportJobDetailByAppId(eaiAppId);
        if (eaiImportJobDetail == null) {
            log.error("No data imported for EAI application: " + eaiAppId);
            throw new RuntimeException("No data imported for EAI application: " + eaiAppId);
        }
        List<String> pathList = eaiImportJobDetail.getPathDetail();
        List<String> processedRecords = eaiImportJobDetail.getPRDetail();
        if (CollectionUtils.isEmpty(pathList) || CollectionUtils.isEmpty(processedRecords)
                || processedRecords.size() != 1 || pathList.size() != 1) {
            log.error(String.format("Should have exactly one extract output, but found %d extract paths and %d counts",
                    CollectionUtils.size(pathList), CollectionUtils.size(processedRecords)));
            throw new RuntimeException("Error in extract info, skip register data table!");
        }
        String resultPath = extractResultPath(eaiImportJobDetail);
        copyErrorFile(customerSpace, upload, dropBoxSummary, eaiImportJobDetail, resultPath);
        updateUploadStatistics(eaiImportJobDetail);
    }

    private void copyErrorFile(String customerSpace, UploadDetails upload, DropBoxSummary dropBoxSummary,
                               EaiImportJobDetail eaiImportJobDetail, String extractPath) {
        if (eaiImportJobDetail.getIgnoredRows().intValue() > 0) {
            String dropFolder = UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox());
            String uploadErrorDir = UploadS3PathBuilderUtils.getUploadImportErrorResultDir(configuration.getProjectId(),
                    configuration.getSourceId(), upload.getDisplayName(), upload.getUploadConfig().getUploadTimestamp());
            String uploadErrorDirKey = UploadS3PathBuilderUtils.combinePath(false, false, dropFolder, uploadErrorDir);
            if (!s3Service.objectExist(dropBoxSummary.getBucket(), uploadErrorDirKey)) {
                s3Service.createFolder(dropBoxSummary.getBucket(), uploadErrorDirKey);
            }
            String errorFileName = getErrorFileName(upload.getUploadConfig().getUploadRawFilePath());

            upload.getUploadConfig().setUploadImportedErrorFilePath(
                    UploadS3PathBuilderUtils.combinePath(false, false, uploadErrorDir, errorFileName));
            String path = extractPath;
            if (!path.endsWith("/")) {
                path += "/";
            }
            log.info("Error file path: " + path);
            String errorFile = path + ERROR_FILE;
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, errorFile)) {
                    copyToS3(errorFile, dropBoxSummary.getBucket(),
                            UploadS3PathBuilderUtils.combinePath(false, false, uploadErrorDirKey, errorFileName));
                    upload.getUploadConfig().getDownloadableFiles().add(DownloadFileType.IMPORT_ERRORS);
                } else {
                    log.error("Cannot find error file under: " + errorFile);
                }
            } catch (IOException e) {
                throw new RuntimeException("Cannot process Error file!");
            }
            uploadProxy.updateUploadConfig(customerSpace, upload.getUploadId(), upload.getUploadConfig());
        }
    }

    private String getErrorFileName(String rawFilePath) {
        String rawFileName = FilenameUtils.getName(rawFilePath);
        String extension = FilenameUtils.getExtension(rawFileName);
        return FilenameUtils.getBaseName(rawFileName) + ERROR_SUFFIX + FilenameUtils.EXTENSION_SEPARATOR + extension;
    }

    private void copyToS3(String hdfsPath, String s3Bucket, String s3Path) throws IOException {
        log.info("Copy from " + hdfsPath + " to " + s3Path);
        long fileSize = HdfsUtils.getFileSize(yarnConfiguration, hdfsPath);
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AmazonS3Exception.class), null);
        retry.execute(context -> {
            if (context.getRetryCount() > 0) {
                log.info(String.format("(Attempt=%d) Retry copying file from hdfs://%s to s3://%s/%s", //
                        context.getRetryCount() + 1, hdfsPath, s3Bucket, s3Path));
            }
            try (InputStream stream = HdfsUtils.getInputStream(yarnConfiguration, hdfsPath)) {
                s3Service.uploadInputStreamMultiPart(s3Bucket, s3Path, stream, fileSize);
            }
            return true;
        });
    }
}
