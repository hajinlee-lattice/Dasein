package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.apps.cdl.service.S3ImportMessageService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.DropBox;
import com.latticeengines.domain.exposed.cdl.S3ImportMessage;

public class S3ImportMessageServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private S3ImportMessageService s3ImportMessageService;

    @Inject
    private DropBoxService dropboxService;

    private static String BUCKET = "latticeengines-qa-customers";
    private static String KEY1 = "dropfolder/%s/Templates/AccountData/%s";
    private static String KEY2 = "dropfolder/%s/DefaultSystem/Templates/ContactData/%s";

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @Test(groups = "functional")
    public void testS3Message() throws InterruptedException {
        DropBox dropBox = dropboxService.create();
        String prefix = dropBox.getDropBox();
        String key1 = String.format(KEY1, prefix, "file1.csv");
        String key2 = String.format(KEY2, prefix, "file2.csv");
        String key3 = String.format(KEY1, prefix, "file1_1.csv");
        s3ImportMessageService.createOrUpdateMessage(BUCKET, key1);
        Thread.sleep(1000L);
        s3ImportMessageService.createOrUpdateMessage(BUCKET, key2);
        Thread.sleep(1000L);
        s3ImportMessageService.createOrUpdateMessage(BUCKET, key3);
        Thread.sleep(1000L);
        List<S3ImportMessage> messages = s3ImportMessageService.getMessageGroupByDropBox();
        Assert.assertEquals(1, messages.size());
        S3ImportMessage s3ImportMessage = messages.get(0);
        Assert.assertEquals(s3ImportMessage.getBucket(), BUCKET);
        Assert.assertEquals(s3ImportMessage.getKey(), key1);
        Assert.assertEquals(s3ImportMessage.getDropBox().getDropBox(), prefix);
        s3ImportMessageService.deleteMessage(s3ImportMessage);
        Thread.sleep(1000L);
        messages = s3ImportMessageService.getMessageGroupByDropBox();
        Assert.assertEquals(1, messages.size());
        s3ImportMessage = messages.get(0);
        Assert.assertEquals(s3ImportMessage.getBucket(), BUCKET);
        Assert.assertEquals(s3ImportMessage.getKey(), key2);
        Assert.assertEquals(s3ImportMessage.getDropBox().getDropBox(), prefix);
    }
}
