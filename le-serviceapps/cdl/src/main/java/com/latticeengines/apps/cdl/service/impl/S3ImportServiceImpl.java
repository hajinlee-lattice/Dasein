package com.latticeengines.apps.cdl.service.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.apps.cdl.service.S3ImportMessageService;
import com.latticeengines.apps.cdl.service.S3ImportService;
import com.latticeengines.apps.cdl.util.S3ImportMessageUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.S3ImportMessage;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;

@Component("s3ImportService")
public class S3ImportServiceImpl implements S3ImportService {

    private static final Logger log = LoggerFactory.getLogger(S3ImportServiceImpl.class);

    private static final String SOURCE = "File";

    @Inject
    private S3ImportMessageService s3ImportMessageService;

    @Inject
    private DropBoxService dropBoxService;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Override
    public boolean saveImportMessage(String bucket, String key, String hostUrl) {
        S3ImportMessage message = s3ImportMessageService.createOrUpdateMessage(bucket, key, hostUrl);
        return message != null;
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
                DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(tenantId, SOURCE, feedType);
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
                if (submitApplication(tenantId, message.getBucket(), feedType, message.getKey(), message.getHostUrl())) {
                    dropBoxSet.add(message.getDropBox().getDropBox());
                    s3ImportMessageService.deleteMessage(message);
                }
            } catch (RuntimeException e) {
                log.error(String.format("Cannot submit import for: %s", message.getKey()), e);
            }
        }
        return false;
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
}
