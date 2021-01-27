package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.apps.cdl.service.S3ImportMessageService;
import com.latticeengines.apps.cdl.service.S3ImportService;
import com.latticeengines.apps.core.service.DropBoxService;
import com.latticeengines.apps.core.util.S3ImportMessageUtils;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DataOperationRequest;
import com.latticeengines.domain.exposed.cdl.ListSegmentImportRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportMessage;
import com.latticeengines.domain.exposed.dcp.DCPImportRequest;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.jms.S3ImportMessageType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.dcp.SourceProxy;
import com.latticeengines.proxy.exposed.dcp.UploadProxy;

@Component("s3ImportService")
public class S3ImportServiceImpl implements S3ImportService {

    private static final Logger log = LoggerFactory.getLogger(S3ImportServiceImpl.class);

    private static final Long MESSAGE_REMOVE_THRESHOLD = TimeUnit.DAYS.toMillis(1);

    private static final String SOURCE = "File";
    private static final String TEMPLATES = "Templates";
    private static final String DATA_OPERATION = "Data_Operation";
    private static final String DROPFOLDER = "dropfolder";
    private static final String STACK_INFO_URL = "/pls/health/stackinfo";

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    @Inject
    private S3ImportMessageService s3ImportMessageService;

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private BatonService batonService;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private SourceProxy sourceProxy;

    @Value("${cdl.app.public.url}")
    private String appPublicUrl;

    @Value("${dcp.app.public.url}")
    private String dcpPublicUrl;

    @Value("${common.le.stack}")
    private String currentStack;

    @Value("${common.microservice.url}")
    private String hostUrl;

    @Value("${common.stack.is.dcp}")
    private boolean isDCPStack;

    @Override
    public boolean saveImportMessage(String bucket, String key, S3ImportMessageType messageType) {
        if (isValidKey(key, messageType)) {
            S3ImportMessage message = s3ImportMessageService.createOrUpdateMessage(bucket, key, messageType);
            return message != null;
        } else {
            log.warn(String.format("Not a valid import message for key: %s, skip save import message", key));
            return false;
        }
    }

    private boolean isValidKey(String key, S3ImportMessageType messageType) {
        if (StringUtils.isEmpty(key)) {
            return false;
        }
        String[] parts = key.split("/");
        if (S3ImportMessageType.Atlas.equals(messageType)) {
            if (parts.length < 5) {
                return false;
            }
            if (!DROPFOLDER.equals(parts[0])) {
                return false;
            }
            if (parts.length == 5) {
                if (!TEMPLATES.equals(parts[2])) {
                    return false;
                }
                return S3ImportMessageUtils.validImportFileTypes.stream().anyMatch(str -> parts[4].toLowerCase().endsWith(str));
            } else if (parts.length == 6) {
                if (!TEMPLATES.equals(parts[3])) {
                    return false;
                }
                return S3ImportMessageUtils.validImportFileTypes.stream().anyMatch(str -> parts[5].toLowerCase().endsWith(str));
            } else {
                return false;
            }
        } else if (S3ImportMessageType.DCP.equals(messageType)) {
            return parts[parts.length - 1].toLowerCase().endsWith(".csv");
        } else if (S3ImportMessageType.LISTSEGMENT.equals(messageType)) {
            return parts[parts.length - 1].toLowerCase().endsWith(".zip");
        } else if (S3ImportMessageType.DATAOPERATION.equals(messageType)) {
            if (!DROPFOLDER.equals(parts[0])) {
                return false;
            }
            if (!DATA_OPERATION.equals(parts[2])) {
                return false;
            }
            return parts[parts.length - 1].toLowerCase().endsWith(".csv");
        }
        else {
            return false;
        }
    }

    @Override
    public boolean submitImportJob() {
        List<S3ImportMessage> messageList = s3ImportMessageService.getMessageGroupByDropBox();
        if (CollectionUtils.isEmpty(messageList)) {
            log.info("There's no import message that needs to process!");
            return true;
        }
        Set<String> dropBoxSet = new HashSet<>();
        for (S3ImportMessage message : messageList) {
            try {
                log.info("Start processing : " + message.getKey());
                if (S3ImportMessageType.LISTSEGMENT.equals(message.getMessageType())){
                    String segmentIndex = S3ImportMessageUtils.getSegmentIndex(message);
                    if (dropBoxSet.contains(segmentIndex)) {
                        log.info(String.format("Already submitted one import for segment: %s",
                                segmentIndex));
                        continue;
                    }
                } else if (dropBoxSet.contains(message.getDropBox().getDropBox())) {
                    log.info(String.format("Already submitted one import for dropBox: %s",
                            message.getDropBox().getDropBox()));
                    continue;
                }
                if (message.getMessageType() == null || S3ImportMessageType.Atlas.equals(message.getMessageType())) {
                    submitAtlasImport(dropBoxSet, message);
                } else if (S3ImportMessageType.DCP.equals(message.getMessageType())) {
                    submitDCPImport(dropBoxSet, message);
                } else if (S3ImportMessageType.LISTSEGMENT.equals(message.getMessageType())) {
                    submitListSegmentImport(dropBoxSet, message);
                } else if (S3ImportMessageType.DATAOPERATION.equals(message.getMessageType())) {
                    submitDataOperation(dropBoxSet, message);
                }
            } catch (RuntimeException e) {
                // Only log message instead of stack trace to reduce log.
                log.error(String.format("Cannot submit import for: %s, error: %s", message.getKey(), e.getMessage()));
                if (System.currentTimeMillis() - message.getUpdated().getTime() > MESSAGE_REMOVE_THRESHOLD) {
                    log.warn(String.format("Remove message %s from import message queue due to timeout.",
                            message.getKey()));
                    s3ImportMessageService.deleteMessage(message);
                }
            }
        }
        return true;
    }

    @Override
    public void updateMessageUrl() {
        List<S3ImportMessage> messageList = new ArrayList<>();
        if (isDCPStack) {
            messageList = s3ImportMessageService.getMessageWithoutHostUrlByType(S3ImportMessageType.DCP);
        } else {
            messageList = s3ImportMessageService.getMessageWithoutHostUrlByType(S3ImportMessageType.Atlas);
            List<S3ImportMessage> messageListSegment = s3ImportMessageService.getMessageWithoutHostUrlByType(S3ImportMessageType.LISTSEGMENT);
            List<S3ImportMessage> messageDataOperation = s3ImportMessageService.getMessageWithoutHostUrlByType(S3ImportMessageType.DATAOPERATION);
            messageList.addAll(messageListSegment);
            messageList.addAll(messageDataOperation);
        }

        if (CollectionUtils.isNotEmpty(messageList)) {
            log.info("Current stack: " + currentStack + " hostUrl: " + hostUrl + " isDCPStack: " + isDCPStack);
            for (S3ImportMessage message : messageList) {
                String tenantId;
                if (!S3ImportMessageType.LISTSEGMENT.equals(message.getMessageType())) {
                    Tenant tenant = dropBoxService.getDropBoxOwner(message.getDropBox().getDropBox());
                    if (tenant == null) {
                        log.error("Cannot find DropBox Owner: " + message.getDropBox().getDropBox());
                        continue;
                    }
                    tenantId = CustomerSpace.shortenCustomerSpace(tenant.getId());
                } else {
                    tenantId = S3ImportMessageUtils.getKeyPart(message.getKey(), S3ImportMessageType.LISTSEGMENT,
                            S3ImportMessageUtils.KeyPart.TENANT_ID);
                }
                if (shouldSet(tenantId, message.getMessageType())) {
                    log.info("Set message " + message.getKey() + " with hostUrl: " + hostUrl);
                    s3ImportMessageService.updateHostUrl(message.getKey(), hostUrl);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private boolean shouldSet(String tenantId, S3ImportMessageType messageType) {
        String url = (S3ImportMessageType.Atlas.equals(messageType) || S3ImportMessageType.LISTSEGMENT.equals(messageType)
                || S3ImportMessageType.DATAOPERATION.equals(messageType)) ?
                appPublicUrl + STACK_INFO_URL : dcpPublicUrl + STACK_INFO_URL;
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        boolean currentActive = true;
        boolean inactiveImport = false;
        try {
            inactiveImport = batonService.isEnabled(customerSpace, LatticeFeatureFlag.AUTO_IMPORT_ON_INACTIVE);
        } catch (Exception e) {
            log.warn("Cannot get AUTO_IMPORT_ON_INACTIVE flag for " + tenantId + "default running on active stack");
        }
        try {
            RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                    Collections.singleton(HttpServerErrorException.class), null);
            AtomicReference<Map<String, String>> stackInfo = new AtomicReference<>();
            retry.execute(retryContext -> {
                stackInfo.set(restTemplate.getForObject(url, Map.class));
                return true;
            });
            if (MapUtils.isNotEmpty(stackInfo.get()) && stackInfo.get().containsKey("CurrentStack")) {
                String activeStack = stackInfo.get().get("CurrentStack");
                currentActive = currentStack.equalsIgnoreCase(activeStack);
                log.info("Message type: " + messageType + " Current stack: " + currentStack + " Active stack: " + activeStack +
                        " INACTIVE IMPORT=" + inactiveImport);
            }
            return currentActive ^ inactiveImport;
        } catch (Exception e) {
            // active stack is down, running on inactive
            log.warn("Cannot get active stack!", e);
            return inactiveImport;
        }
    }

    private void submitDCPImport(Set<String> dropBoxSet, S3ImportMessage message) {
        log.info(String.format("DCP import for %s / %s", message.getBucket(), message.getKey()));
        String projectId = S3ImportMessageUtils.getKeyPart(message.getKey(), S3ImportMessageType.DCP,
                S3ImportMessageUtils.KeyPart.PROJECT_ID);
        String sourceId = S3ImportMessageUtils.getKeyPart(message.getKey(), S3ImportMessageType.DCP,
                S3ImportMessageUtils.KeyPart.SOURCE_ID);
        String fileName = S3ImportMessageUtils.getKeyPart(message.getKey(), S3ImportMessageType.DCP,
                S3ImportMessageUtils.KeyPart.FILE_NAME);

        Tenant tenant = dropBoxService.getDropBoxOwner(message.getDropBox().getDropBox());
        String tenantId = CustomerSpace.shortenCustomerSpace(tenant.getId());
        log.info(String.format("Process DCP import with Tenant %s, Project %s, Source %s, File %s", tenantId, projectId,
                sourceId, fileName));
        Source source = sourceProxy.getSource(tenantId, sourceId);
        if (source == null) {
            log.info("Source not setup yet for key: {}", message.getKey());
            return;
        }
        if (DataFeedTask.S3ImportStatus.Pause.equals(source.getImportStatus())) {
            log.info("Import paused for source: {}", sourceId);
            return;
        }
        if (submitDCPImport(tenantId, projectId, sourceId, message.getKey(), message.getHostUrl())) {
            dropBoxSet.add(message.getDropBox().getDropBox());
            s3ImportMessageService.deleteMessage(message);
        }
    }

    private void submitAtlasImport(Set<String> dropBoxSet, S3ImportMessage message) {
        String feedType = S3ImportMessageUtils.getFeedTypeFromKey(message.getKey());
        log.info("FeedType: " + feedType);
        Tenant tenant = dropBoxService.getDropBoxOwner(message.getDropBox().getDropBox());
        log.info("Tenant: " + tenant.getId());
        String tenantId = CustomerSpace.shortenCustomerSpace(tenant.getId());
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(tenantId, SOURCE, feedType);
        if (dataFeedTask == null) {
            log.info(String.format("Template not exist for key: %s feedType %s", message.getKey(), feedType));
            return;
        }
        if (DataFeedTask.S3ImportStatus.Pause.equals(dataFeedTask.getS3ImportStatus())) {
            log.info(String.format("Import paused for template: %s", dataFeedTask.getUniqueId()));
            return;
        }
        log.info(String.format("S3 import for %s / %s", message.getBucket(), message.getKey()));
        if (submitApplication(tenantId, message.getBucket(), feedType, message.getKey(), message.getHostUrl())) {
            dropBoxSet.add(message.getDropBox().getDropBox());
            s3ImportMessageService.deleteMessage(message);
        }
    }

    private boolean submitApplication(String tenantId, String bucket, String feedType, String key, String hostUrl) {
        S3FileToHdfsConfiguration config = new S3FileToHdfsConfiguration();
        config.setFeedType(feedType);
        config.setS3Bucket(bucket);
        config.setS3FilePath(key);
        try {
            CDLProxy cdlProxy = new CDLProxy(hostUrl);
            ApplicationId applicationId =  cdlProxy.submitS3ImportJob(tenantId, config);
            log.info("Start S3 file import by applicationId : " + applicationId.toString());
            return true;
        } catch(LedpException e) {
            log.error("S3 import file validation failed!", e);
            return true;
        } catch (Exception e) {
            log.error("Failed to submit s3 import job.", e);
            return false;
        }
    }

    private boolean submitDCPImport(String tenantId, String projectId, String sourceId, String key, String hostUrl) {
        DCPImportRequest request = new DCPImportRequest();
        request.setProjectId(projectId);
        request.setSourceId(sourceId);
        request.setS3FileKey(key);
        request.setUserId("s3 user");
        try {
            UploadProxy uploadProxy = new UploadProxy(hostUrl);
            ApplicationId applicationId = uploadProxy.startImport(tenantId, request);
            log.info("Start DCP file import by applicationId : " + applicationId.toString());
            return true;
        } catch (Exception e) {
            log.error("Failed to submit dcp import job.", e);
            return false;
        }

    }

    private void submitListSegmentImport(Set<String> dropBoxSet, S3ImportMessage message) {
        log.info(String.format("ListSegment import for %s / %s", message.getBucket(), message.getKey()));
        String tenantId = S3ImportMessageUtils.getKeyPart(message.getKey(), S3ImportMessageType.LISTSEGMENT,
                S3ImportMessageUtils.KeyPart.TENANT_ID);
        String segmentName = S3ImportMessageUtils.getKeyPart(message.getKey(), S3ImportMessageType.LISTSEGMENT,
                S3ImportMessageUtils.KeyPart.SEGMENT_NAME);
        String fileName = S3ImportMessageUtils.getKeyPart(message.getKey(), S3ImportMessageType.LISTSEGMENT,
                S3ImportMessageUtils.KeyPart.FILE_NAME);

        log.info(String.format("Process ListSegment import with Tenant %s, segment %s, File %s", tenantId, segmentName,
                fileName));
        if (submitListSegmentImport(tenantId, segmentName, message.getKey(), message.getHostUrl())) {
            dropBoxSet.add(S3ImportMessageUtils.getSegmentIndex(message));
            s3ImportMessageService.deleteMessage(message);
        }
    }

    private boolean submitListSegmentImport(String tenantId, String segmentName, String key, String hostUrl) {
        ListSegmentImportRequest request = new ListSegmentImportRequest();
        request.setSegmentName(segmentName);
        request.setS3FileKey(key);
        try {
            CDLProxy cdlProxy = new CDLProxy(hostUrl);
            ApplicationId applicationId = cdlProxy.importListSegment(tenantId, request);
            log.info("Start segment import by applicationId : " + applicationId.toString());
            return true;
        } catch (Exception e) {
            log.error("Failed to submit segment import job.", e);
            return false;
        }

    }

    private void submitDataOperation(Set<String> dropBoxSet, S3ImportMessage message) {
        Tenant tenant = dropBoxService.getDropBoxOwner(message.getDropBox().getDropBox());
        log.info("Tenant: " + tenant.getId());
        String tenantId = CustomerSpace.shortenCustomerSpace(tenant.getId());
        String dropPath = S3ImportMessageUtils.getDropPathFromMessage(message);
        log.info("DropPath: " + dropPath);
        log.info(String.format("S3 data operation for %s/%s", message.getBucket(), message.getKey()));
        if (submitDataOperationApplication(tenantId, message.getBucket(), message.getKey(), dropPath, message.getHostUrl())) {
            dropBoxSet.add(message.getDropBox().getDropBox());
            s3ImportMessageService.deleteMessage(message);
        }
    }

    private boolean submitDataOperationApplication(String tenantId, String bucket, String key, String dropPath, String hostUrl) {
        DataOperationRequest dataOperationRequest = new DataOperationRequest();
        dataOperationRequest.setS3Bucket(bucket);
        dataOperationRequest.setS3DropPath(dropPath);
        dataOperationRequest.setS3FileKey(key);
        try {
            CDLProxy cdlProxy = new CDLProxy(hostUrl);
            ApplicationId applicationId =  cdlProxy.submitDataOperationJob(tenantId, dataOperationRequest);
            log.info("Start data operation job by applicationId : " + applicationId.toString());
            return true;
        } catch(LedpException e) {
            log.error("Data operation file validation failed!", e);
            return true;
        } catch (Exception e) {
            log.error("Failed to submit data operation job.", e);
            return false;
        }
    }
}
