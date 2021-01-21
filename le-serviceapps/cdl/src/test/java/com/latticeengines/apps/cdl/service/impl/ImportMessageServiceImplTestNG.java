package com.latticeengines.apps.cdl.service.impl;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.ImportMessageService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.ImportMessage;
import com.latticeengines.domain.exposed.jms.S3ImportMessageType;

public class ImportMessageServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ImportMessageServiceImplTestNG.class);

    @Inject
    private ImportMessageService importMessageService;

    private RetryTemplate retry;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        retry = RetryUtils.getRetryTemplate(10, Collections.singleton(AssertionError.class), null);
    }

    @Test(groups = "functional")
    public void testCRUD() {
        String bucket = "bucket";
        String key = "key";
        String sourceId = UUID.randomUUID().toString();
        S3ImportMessageType messageType = S3ImportMessageType.INBOUND_CONNECTION;
        AtomicReference<ImportMessage> importMessage = new AtomicReference<>(new ImportMessage());
        importMessageService.createOrUpdateImportMessage(createImportMessage(bucket, key, sourceId, messageType));
        retry.execute(context -> {
            importMessage.set(importMessageService.getBySourceId(sourceId));
            Assert.assertNotNull(importMessage.get());
            Assert.assertEquals(importMessage.get().getBucket(), bucket);
            Assert.assertEquals(importMessage.get().getKey(), key);
            return true;
        });
    }

    private ImportMessage createImportMessage(String bucket, String key, String sourceId, S3ImportMessageType messageType) {
        ImportMessage importMessage = new ImportMessage();
        importMessage.setBucket(bucket);
        importMessage.setKey(key);
        importMessage.setSourceId(sourceId);
        importMessage.setMessageType(messageType);
        return importMessage;
    }

}
