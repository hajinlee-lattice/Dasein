package com.latticeengines.dcp.workflow.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.dcp.Upload;
import com.latticeengines.domain.exposed.dcp.UploadStats;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.dcp.steps.ImportSourceStepConfiguration;
import com.latticeengines.domain.exposed.util.UploadS3PathBuilderUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;
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
    private WorkflowProxy workflowProxy;

    @Inject
    private S3Service s3Service;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    @Override
    public void execute() {
        log.info("Start import DCP file");
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        Upload upload = uploadProxy.getUpload(customerSpace.toString(), configuration.getUploadPid());
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
                             Upload upload) {
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
        importStats.setSuccessCnt(totalCnt - errorCnt);
        importStats.setErrorCnt(errorCnt);
        UploadStats stats = new UploadStats();
        stats.setImportStats(importStats);
        putObjectInContext(UPLOAD_STATS, stats);
    }

    private void registerResultAsATable(EaiImportJobDetail jobDetail) {
        HdfsDataUnit result = new HdfsDataUnit();
        result.setDataFormat(DataUnit.DataFormat.AVRO);

        List<String> paths = JsonUtils.convertList((List<?>) jobDetail.getDetails().get("ExtractPathList"), String.class);
        if (CollectionUtils.isEmpty(paths) || paths.size() != 1) {
            throw new RuntimeException("Should have exactly one extract path, but found {}" + CollectionUtils.size(paths));
        } else {
            String path = PathUtils.toParquetOrAvroDir(paths.get(0));
            log.info("Setting data unit path to " + path);
            result.setPath(path);
        }
        List<String> counts = JsonUtils.convertList((List<?>) jobDetail.getDetails().get("ProcessedRecordsList"), String.class);
        if (CollectionUtils.isEmpty(counts) || counts.size() != paths.size()) {
            log.info("Cannot determine counts from eai job detail.");
        } else {
            result.setCount(Long.parseLong(counts.get(0)));
        }

        String tableName = NamingUtils.timestamp("ImportResult");
        CustomerSpace customerSpace = configuration.getCustomerSpace();
        Table table = SparkUtils.hdfsUnitToTable(tableName, InterfaceName.InternalId.name(), result, yarnConfiguration, podId, customerSpace);
        metadataProxy.createTable(customerSpace.toString(), tableName, table);
        Table eventTable = metadataProxy.getTable(customerSpace.toString(), tableName);
        putObjectInContext(PREMATCH_UPSTREAM_EVENT_TABLE, eventTable);
    }

    private S3FileToHdfsConfiguration setupConfiguration(DataFeedTask dataFeedTask, DropBoxSummary dropBoxSummary,
                                                         Upload upload) {
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

    private void updateStats(String customerSpace, String eaiAppId, Upload upload,
                             DropBoxSummary dropBoxSummary) {
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
        updateUploadStatistics(eaiImportJobDetail);
        registerResultAsATable(eaiImportJobDetail);

        copyErrorFile(customerSpace, upload, dropBoxSummary, eaiImportJobDetail);

        uploadProxy.updateUploadStatus(customerSpace, upload.getPid(), Upload.Status.MATCH_STARTED);
    }

    private void copyErrorFile(String customerSpace, Upload upload, DropBoxSummary dropBoxSummary, EaiImportJobDetail eaiImportJobDetail) {
        if (eaiImportJobDetail.getIgnoredRows().intValue() > 0) {
            String dropFolder = UploadS3PathBuilderUtils.getDropFolder(dropBoxSummary.getDropBox());
            String uploadErrorDir = UploadS3PathBuilderUtils.getUploadImportErrorResultDir(configuration.getProjectId(),
                    configuration.getSourceId(), upload.getUploadConfig().getUploadTSPrefix());
            String uploadErrorDirKey = UploadS3PathBuilderUtils.combinePath(false, false, dropFolder, uploadErrorDir);
            if (!s3Service.objectExist(dropBoxSummary.getBucket(), uploadErrorDirKey)) {
                s3Service.createFolder(dropBoxSummary.getBucket(), uploadErrorDirKey);
            }
            String errorFileName = getErrorFileName(upload.getUploadConfig().getUploadRawFilePath());

            upload.getUploadConfig().setUploadImportedErrorFilePath(
                    UploadS3PathBuilderUtils.combinePath(false, false, uploadErrorDirKey, errorFileName));
            List<String> pathList = eaiImportJobDetail.getPathDetail();
            String path = pathList.get(0);
            String dirPath = path.substring(0, path.lastIndexOf("*.avro"));
            log.info("Diagnostic file path: " + dirPath);
            String errorFile = "";
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, dirPath + ERROR_FILE)) {
                    Matcher matcher = Pattern.compile("^hdfs://(?<cluster>[^/]+)/Pods/(?<tail>.*)").matcher(dirPath);
                    if (matcher.matches()) {
                        errorFile = "/Pods/" + matcher.group("tail") + ERROR_FILE;
                    } else {
                        errorFile = dirPath + ERROR_FILE;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Cannot process Error file!");
            }
            if (StringUtils.isNotBlank(errorFile)) {
                log.info("Import error file path: " + errorFile);
                putStringValueInContext(IMPORT_ERROR_FILE, errorFile);
            }
            uploadProxy.updateUploadConfig(customerSpace, upload.getPid(), upload.getUploadConfig());
        }
    }

    private String getErrorFileName(String rawFilePath) {
        String rawFileName = FilenameUtils.getName(rawFilePath);
        String extension = FilenameUtils.getExtension(rawFileName);
        return FilenameUtils.getBaseName(rawFileName) + ERROR_SUFFIX + FilenameUtils.EXTENSION_SEPARATOR + extension;
    }
}
