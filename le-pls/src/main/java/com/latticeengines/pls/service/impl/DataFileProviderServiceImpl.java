package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.app.exposed.download.BundleFileHttpDownloader;
import com.latticeengines.app.exposed.download.CustomerSpaceHdfsFileDownloader;
import com.latticeengines.app.exposed.download.CustomerSpaceS3FileDownloader;
import com.latticeengines.app.exposed.download.HdfsFileHttpDownloader;
import com.latticeengines.app.exposed.download.HdfsFileHttpDownloader.DownloadRequestBuilder;
import com.latticeengines.app.exposed.download.HttpFileDownLoader;
import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.service.DataFileProviderService;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.CDLAttrConfigProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.lp.SourceFileProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("dataFileProviderService")
public class DataFileProviderServiceImpl implements DataFileProviderService {

    private static final Logger log = LoggerFactory.getLogger(DataFileProviderServiceImpl.class);

    private static String MODEL_PROFILE_AVRO = "model_profile.avro";

    private static String S3_ATLAS_DATA_FILE = "/%s/atlas/Data/Files/%s";

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private SourceFileProxy sourceFileProxy;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private ImportFromS3Service importFromS3Service;

    @Inject
    private CDLAttrConfigProxy cdlAttrConfigProxy;

    @Autowired
    private BatonService batonService;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Value("${aws.customer.s3.bucket}")
    protected String s3Bucket;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    @Override
    public void downloadFile(HttpServletRequest request, HttpServletResponse response, String modelId, String mimeType,
            String filter, HttpFileDownLoader.DownloadMode mode) {
        HdfsFileHttpDownloader downloader = getDownloader(modelId, mimeType, filter);
        downloader.downloadFile(request, response, mode);
    }

    @Override
    public void downloadFile(HttpServletRequest request, HttpServletResponse response, String modelId, String mimeType,
            String filter) {
        downloadFile(request, response, modelId, mimeType, filter, HttpFileDownLoader.DownloadMode.DEFAULT);
    }

    @Override
    public void downloadPivotFile(HttpServletRequest request, HttpServletResponse response, String modelId,
            String mimeType) {
        ModelSummary summary = modelSummaryProxy.findValidByModelId(MultiTenantContext.getTenant().getId(), modelId);
        validateModelSummary(summary, modelId);
        String filePath = summary.getPivotArtifactPath();
        downloadFileByPath(request, response, mimeType, filePath);
    }

    @Override
    public void downloadTrainingSet(HttpServletRequest request, HttpServletResponse response, String modelId, String mimeType) {

        ModelSummary summary = modelSummaryProxy.findValidByModelId(MultiTenantContext.getTenant().getId(), modelId);
        validateModelSummary(summary, modelId);

        String trainingFilePath = summary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TrainingFilePath, "");

        downloadFileByPath(request, response, mimeType, trainingFilePath);

    }

    @Override
    public void downloadModelProfile(HttpServletRequest request, HttpServletResponse response, String modelId,
            String mimeType) throws IOException {
        String customer = MultiTenantContext.getTenant().getId();
        ModelSummary summary = modelSummaryProxy.findValidByModelId(customer, modelId);
        validateModelSummary(summary, modelId);
        String eventColumn = getEventColumn(customer, summary.getEventTableName());
        String modelProfilePath = String.format("%s%s/data/%s-%s-Metadata/%s", modelingServiceHdfsBaseDir,
                summary.getTenant().getId(), summary.getEventTableName(), eventColumn, MODEL_PROFILE_AVRO);
        downloadFileByPath(request, response, mimeType, modelProfilePath);
    }

    private String getEventColumn(String customer, String eventTableName) {
        String eventColumn = "Event";
        Table eventTable = metadataProxy.getTable(customer, eventTableName);
        if (eventTable != null) {
            List<Attribute> events = eventTable.getAttributes(LogicalDataType.Event);
            if (CollectionUtils.isNotEmpty(events)) {
                eventColumn = events.get(0).getDisplayName();
            }
        }
        return eventColumn;
    }

    @Override
    public void downloadFileByApplicationId(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String applicationId, String fileDisplayName) throws IOException {
        log.info(String.format("Download file with applicationId=%s", applicationId));
        SourceFile sourceFile = sourceFileProxy.findByApplicationId(MultiTenantContext.getShortTenantId(),
                applicationId);
        validateSourceFile(sourceFile);
        downloadSourceFileCsv(request, response, mimeType, fileDisplayName, sourceFile);
    }

    @Override
    public void downloadFileByFileName(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String fileName) throws IOException {
        log.info(String.format("Download file with fileName=%s", fileName));
        SourceFile sourceFile = sourceFileProxy.findByName(MultiTenantContext.getShortTenantId(), fileName);
        validateSourceFile(sourceFile);
        downloadSourceFileCsv(request, response, mimeType, sourceFile.getDisplayName(), sourceFile);
    }

    private void validateSourceFile(SourceFile sourceFile) {
        if (sourceFile == null) {
            throw new NullPointerException("source file is null");
        }
    }

    @VisibleForTesting
    void downloadSourceFileCsv(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String fileDisplayName, SourceFile sourceFile) throws IOException {
        boolean fileDownloaded = false;
        if (sourceFile != null) {
            String filePath = sourceFile.getPath();
            if (filePath != null && !filePath.isEmpty()) {
                CustomerSpaceHdfsFileDownloader downloader = getCustomerSpaceDownloader(mimeType, filePath,
                        fileDisplayName);
                downloader.downloadFile(request, response);
                fileDownloaded = true;
            }
        }

        if (!fileDownloaded) {
            throw new IOException(String.format("Error downloading source file with name: %s", fileDisplayName));
        }
    }

    @Override
    public void downloadFileByPath(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String filePath) {
        if (filePath != null && !filePath.isEmpty()) {
            CustomerSpaceHdfsFileDownloader downloader = getCustomerSpaceDownloader(mimeType, filePath, null);
            downloader.downloadFile(request, response);
        }
    }

    private CustomerSpaceHdfsFileDownloader getCustomerSpaceDownloader(String mimeType, String filePath,
            String fileName) {
        CustomerSpaceHdfsFileDownloader.FileDownloadBuilder builder = new CustomerSpaceHdfsFileDownloader.FileDownloadBuilder();
        String customer = MultiTenantContext.getTenant().getId();
        customer = customer != null ? customer : new HdfsToS3PathBuilder().getCustomerFromHdfsPath(filePath);
        builder.setMimeType(mimeType).setFilePath(filePath).setYarnConfiguration(yarnConfiguration)
                .setFileName(fileName).setCustomer(customer).setImportFromS3Service(importFromS3Service)
                .setBatonService(batonService);
        return new CustomerSpaceHdfsFileDownloader(builder);
    }

    @Override
    public String getFileContents(String modelId, String mimeType, String filter) throws Exception {
        HdfsFileHttpDownloader downloader = getDownloader(modelId, mimeType, filter);
        return downloader.getFileContents();
    }

    private HdfsFileHttpDownloader getDownloader(String modelId, String mimeType, String filter) {

        DownloadRequestBuilder requestBuilder = new DownloadRequestBuilder();
        requestBuilder.setMimeType(mimeType).setFilter(filter).setModelId(modelId)
                .setYarnConfiguration(yarnConfiguration).setModelSummaryProxy(modelSummaryProxy);
        requestBuilder.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir)
                .setImportFromS3Service(importFromS3Service).setCDLAttrConfigProxy(cdlAttrConfigProxy)
                .setBatonService(batonService);
        return new HdfsFileHttpDownloader(requestBuilder);
    }

    private void validateModelSummary(ModelSummary summary, String modelId) {
        if (summary == null) {
            throw new NullPointerException(String.format("Modelsummary with id %s is null", modelId));
        }
    }

    @VisibleForTesting
    void setConfiguration(Configuration configuration) {
        this.yarnConfiguration = configuration;
    }

    @VisibleForTesting
    void setModelSummaryProxy(ModelSummaryProxy modelSummaryProxy) {
        this.modelSummaryProxy = modelSummaryProxy;
    }

    @VisibleForTesting
    void setModelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
        this.modelingServiceHdfsBaseDir = modelingServiceHdfsBaseDir;
    }

    @VisibleForTesting
    void setImportFromS3Service(ImportFromS3Service importFromS3Service) {
        this.importFromS3Service = importFromS3Service;
    }

    @VisibleForTesting
    void setBatonService(BatonService batonService) {
        this.batonService = batonService;
    }

    @Override
    public void downloadS3File(HttpServletRequest request, HttpServletResponse response, String mimeType,
            String fileName, String filePath, String bucketName) throws IOException {
        log.info(String.format("Download file with fileName %s and filePath %s.", fileName, filePath));
        CustomerSpaceS3FileDownloader.S3FileDownloadBuilder builder = new CustomerSpaceS3FileDownloader.S3FileDownloadBuilder();
        builder.setMimeType(mimeType).setFilePath(filePath).setFileName(fileName).setBucketName(bucketName)
                .setImportFromS3Service(importFromS3Service).setBatonService(batonService);
        CustomerSpaceS3FileDownloader customerSpaceS3FileDownloader = new CustomerSpaceS3FileDownloader(builder);
        customerSpaceS3FileDownloader.downloadFile(request, response);

    }

    @Override
    public void downloadCurrentBundleFile(HttpServletRequest request, HttpServletResponse response, String mimeType) {
        String fileName = "currentBundle.csv";
        BundleFileHttpDownloader.BundleFileHttpDownloaderBuilder builder =
                new BundleFileHttpDownloader.BundleFileHttpDownloaderBuilder();
        builder.setMimeType(mimeType).setFileName(fileName).setBucketName(s3Bucket).setDataCollectionProxy(dataCollectionProxy)
                .setImportFromS3Service(importFromS3Service).setPodId(podId).setConfiguration(yarnConfiguration);
        BundleFileHttpDownloader downloader = new BundleFileHttpDownloader(builder);
        downloader.downloadFile(request, response);

    }

    @Override
    public void downloadPostMatchFile(HttpServletRequest request, HttpServletResponse response, String modelId, String filter) {
        //model
        ModelSummary summary = modelSummaryProxy.findValidByModelId(MultiTenantContext.getTenant().getId(), modelId);
        validateModelSummary(summary, modelId);

        //event column and transform
        String customer = MultiTenantContext.getTenant().getId();
        String eventColumn = getEventColumn(customer, summary.getEventTableName());
        Map<String, String> headerTransform = null;
        log.info("event column name = " + eventColumn);
        if (!eventColumn.equals("Event")) {
            headerTransform = new HashMap<>();
            headerTransform.put(eventColumn, "Event");
        }

        //download
        HdfsFileHttpDownloader downloader = getDownloader(modelId, MediaType.APPLICATION_OCTET_STREAM, filter);
        downloader.downloadCsvWithTransform(request, response, headerTransform);
    }

    @Override
    public void downloadCurrentBundleFileV2(HttpServletRequest request, HttpServletResponse response, String mimeType
            , String customerSpace) throws Exception {
        List<Action> actions = actionProxy.getActions(customerSpace);
        Preconditions.checkState(CollectionUtils.isNotEmpty(actions), "No bundle file due to empty actions");

        List<Action> importActionsAfterPA =
                actions.stream().filter(action -> ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType())
                        && ActionStatus.ACTIVE.equals(action.getActionStatus()) && action.getOwnerId() != null).collect(Collectors.toList());

        Preconditions.checkState(CollectionUtils.isNotEmpty(importActionsAfterPA), "No bundle file due to empty " +
                "import action");

        // group by owner ID
        Map<Long, List<Action>> ownerIdToList =
                importActionsAfterPA.stream().collect(Collectors.groupingBy(Action::getOwnerId));
        // sort by owner ID DESC
        List<Long> ownerIdList = ownerIdToList.keySet().stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList());
        // only one valid bundle file action
        Action bundleAction = null;
        for (Long ownerId : ownerIdList) {
            Job qaJob = workflowProxy.getJobByWorkflowJobPid(customerSpace, ownerId);
            // find the latest successful PA with bundle
            if (JobStatus.COMPLETED.equals(qaJob.getJobStatus())) {
                List<Action> importActions = ownerIdToList.get(ownerId);
                int bundleCnt = 0;
                bundleAction = null;
                for (Action action : importActions) {
                    ImportActionConfiguration importActionConfiguration = (ImportActionConfiguration) action.getActionConfiguration();
                    String dataFeedTaskId = importActionConfiguration.getDataFeedTaskId();
                    if (dataFeedTaskId == null) {
                        continue;
                    }
                    DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, dataFeedTaskId);
                    if (dataFeedTask == null) {
                        continue;
                    }
                    if (BusinessEntity.Product.name().equals(dataFeedTask.getEntity()) &&
                            DataFeedTask.SubType.Bundle.equals(dataFeedTask.getSubType())) {
                        bundleCnt++;
                        bundleAction = action;
                    }
                }
                if (bundleCnt == 1) {
                    // found one bundle file
                    break;
                } else if (bundleCnt > 1) {
                    // multiple bundle file in this PA
                    throw new RuntimeException("Can't download bundle file due to Multiple files uploaded to system");
                }
            }
        }
        String fileName = "currentBundle.csv";
        if (bundleAction != null) {
            String filePath = getBundleFilePath(bundleAction, customerSpace);
            log.info(String.format("download file from path %s", fileName));
            downloadS3File(request, response, mimeType, fileName, filePath, s3Bucket);

        } else {
            // no bundle file
            throw new RuntimeException("No bundle file found.");
        }

    }

    /**
     * if import file through s3, get path SOURCE_FILE_PATH in inputs
     * else if import file from page, construct path
     * @param action
     * @param customerSpace
     * @return
     */
    private String getBundleFilePath(Action action, String customerSpace) {
        ImportActionConfiguration importActionConfiguration = (ImportActionConfiguration) action.getActionConfiguration();
        Long workflowId = importActionConfiguration.getWorkflowId();
        Preconditions.checkNotNull(workflowId, "configuration is null for bundle");
        Job job = workflowProxy.getJobByWorkflowJobPid(customerSpace, workflowId);
        Map<String, String> inputs = job.getInputs();
        String filePath = inputs.get(WorkflowContextConstants.Inputs.SOURCE_FILE_PATH);
        if (StringUtils.isNotEmpty(filePath)) {
            return filePath;
        } else {
            String fileName = inputs.get(WorkflowContextConstants.Inputs.SOURCE_FILE_NAME);
            Preconditions.checkNotNull(fileName, "fileName is null for bundle");
            return String.format(S3_ATLAS_DATA_FILE, customerSpace, fileName);
        }

    }
}
