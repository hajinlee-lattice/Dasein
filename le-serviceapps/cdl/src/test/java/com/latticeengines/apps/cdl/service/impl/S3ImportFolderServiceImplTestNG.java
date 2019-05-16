package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.latticeengines.apps.cdl.service.S3ImportFolderService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class S3ImportFolderServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private S3ImportFolderService s3ImportFolderService;

    @Inject
    private S3Service s3Service;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironmentWithDataCollection();
    }

    @Test(groups = "functional", enabled = false)
    public void test() {
        // no longer move file in this service.
        String tenantId = CustomerSpace.parse(mainCustomerSpace).getTenantId();

        String key = s3ImportFolderService.startImport(tenantId, "Account",
                "latticeengines-test-artifacts", "le-serviceapps/cdl/end2end/csv/2/Account_400_1000.csv").getLeft();

        List<S3ObjectSummary> inProgress = s3Service.listObjects(s3ImportFolderService.getBucket(), tenantId +
                "/atlas/rawinput/inprogress");

        Assert.assertTrue(s3Service.objectExist(s3ImportFolderService.getBucket(), key));

        String completeKey = s3ImportFolderService.moveFromInProgressToSucceed(key);

        List<S3ObjectSummary> inProgress2 = s3Service.listObjects(s3ImportFolderService.getBucket(), tenantId +
                "/atlas/rawinput/inprogress");

        Assert.assertTrue(inProgress2.size() < inProgress.size());

        List<S3ObjectSummary> succeed = s3Service.listObjects(s3ImportFolderService.getBucket(), tenantId +
                "/atlas/rawinput/completed/succeeded");
        Assert.assertTrue(succeed.size() > 0);

        Assert.assertFalse(s3Service.objectExist(s3ImportFolderService.getBucket(), key));
        Assert.assertTrue(s3Service.objectExist(s3ImportFolderService.getBucket(), completeKey));


    }
}
