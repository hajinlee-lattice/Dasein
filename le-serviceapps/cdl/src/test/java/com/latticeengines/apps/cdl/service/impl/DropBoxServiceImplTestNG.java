package com.latticeengines.apps.cdl.service.impl;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.DropBoxService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.aws.s3.S3Service;

public class DropBoxServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private DropBoxService dropboxService;

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String customersBucket;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
    }

    @Test(groups = "functional")
    public void testCrud() {
        dropboxService.create();
        String prefix = dropboxService.getDropBoxPrefix();
        Assert.assertTrue(StringUtils.isNotBlank(prefix));
        Assert.assertTrue(s3Service.objectExist(dropboxService.getDropBoxBucket(), prefix + "/"));

        dropboxService.delete();
        Assert.assertFalse(StringUtils.isNotBlank(dropboxService.getDropBoxPrefix()));
        Assert.assertFalse(s3Service.objectExist(dropboxService.getDropBoxBucket(), prefix + "/"));

        dropboxService.delete();
        Assert.assertFalse(StringUtils.isNotBlank(dropboxService.getDropBoxPrefix()));
        Assert.assertFalse(s3Service.objectExist(dropboxService.getDropBoxBucket(), prefix + "/"));
    }

}
