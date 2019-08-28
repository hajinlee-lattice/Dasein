package com.latticeengines.apps.cdl.service.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataFeedTaskManagerService;
import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.apps.cdl.service.ProxyResourceService;
import com.latticeengines.apps.cdl.service.S3ImportMessageService;
import com.latticeengines.apps.cdl.service.S3ImportService;
import com.latticeengines.apps.cdl.util.S3ImportMessageUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportMessage;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("s3ImportService")
public class S3ImportServiceImpl implements S3ImportService {

    private static final Logger log = LoggerFactory.getLogger(S3ImportServiceImpl.class);

    private static final Long MESSAGE_REMOVE_THRESHOLD = TimeUnit.DAYS.toMillis(1);

    private static final String SOURCE = "File";
    private static final String TEMPLATES = "Templates";
    private static final String DROPFOLDER = "dropfolder";

    @Inject
    private S3ImportMessageService s3ImportMessageService;

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private ProxyResourceService proxyResourceService;

    @Inject
    private DataFeedTaskManagerService dataFeedTaskManagerService;

    @Override
    public boolean saveImportMessage(String bucket, String key, String hostUrl) {
        if (isValidKey(key)) {
            S3ImportMessage message = s3ImportMessageService.createOrUpdateMessage(bucket, key, hostUrl);
            return message != null;
        } else {
            log.warn(String.format("Not a valid import message for key: %s, skip save import message", key));
            return false;
        }
    }

    private boolean isValidKey(String key) {
        if (StringUtils.isEmpty(key)) {
            return false;
        }
        String[] parts = key.split("/");
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
            return parts[4].toLowerCase().endsWith(".csv");
        } else if (parts.length == 6) {
            if (!TEMPLATES.equals(parts[3])) {
                return false;
            }
            return parts[5].toLowerCase().endsWith(".csv");
        } else {
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
                String feedType = S3ImportMessageUtils.getFeedTypeFromKey(message.getKey());
                log.info("FeedType: " + feedType);
                Tenant tenant = dropBoxService.getDropBoxOwner(message.getDropBox().getDropBox());
                log.info("Tenant: " + tenant.getId());
                String tenantId = CustomerSpace.shortenCustomerSpace(tenant.getId());
                Tenant previousTenant = MultiTenantContext.getTenant();
                MultiTenantContext.setTenant(tenant);
                DataFeedTask dataFeedTask = proxyResourceService.getDataFeedTask(tenantId, SOURCE, feedType);
                if (dataFeedTask == null) {
                    log.info(String.format("Template not exist for key: %s feedType %s", message.getKey(), feedType));
                    continue;
                }
                if (DataFeedTask.S3ImportStatus.Pause.equals(dataFeedTask.getS3ImportStatus())) {
                    log.info(String.format("Import paused for template: %s", dataFeedTask.getUniqueId()));
                    continue;
                }
                if (dropBoxSet.contains(message.getDropBox().getDropBox())) {
                    log.info(String.format("Already submitted one import for dropBox: %s",
                            message.getDropBox().getDropBox()));
                    continue;
                }
                log.info(String.format("S3 import for %s / %s", message.getBucket(), message.getKey()));
                if (submitApplication(tenantId, message.getBucket(), feedType, message.getKey())) {
                    dropBoxSet.add(message.getDropBox().getDropBox());
                    s3ImportMessageService.deleteMessage(message);
                }
                MultiTenantContext.setTenant(previousTenant);
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
        return false;
    }

    private boolean submitApplication(String tenantId, String bucket, String feedType, String key) {
        S3FileToHdfsConfiguration config = new S3FileToHdfsConfiguration();
        config.setFeedType(feedType);
        config.setS3Bucket(bucket);
        config.setS3FilePath(key);
        try {
            String applicationId = dataFeedTaskManagerService.submitS3ImportJob(tenantId, config);
            log.info("Start S3 file import by applicationId : " + applicationId);
            return true;
        } catch (LedpException e) {
            log.error("S3 import file validation failed!", e);
            return true;
        } catch (Exception e) {
            log.error("Failed to submit s3 import job.", e);
            return false;
        }
    }
}
