package com.latticeengines.app.exposed.service.impl;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.util.StringInputStream;
import com.latticeengines.app.exposed.service.ImportFromS3Service;
import com.latticeengines.app.testframework.AppFunctionalTestNGBase;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class ImportFromS3ServiceImplTestNG extends AppFunctionalTestNGBase {

    private static final CustomerSpace CUSTOMER_SPACE = CustomerSpace
            .parse(ImportFromS3ServiceImplTestNG.class.getSimpleName());

    @Inject
    private ImportFromS3Service importFromS3Service;

    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    private String tenantId;
    private String protocol;

    @BeforeClass(groups = "functional")
    private void setUp() throws IOException {
        tenantId = CUSTOMER_SPACE.getTenantId();
        if (!s3Service.objectExist(s3Bucket, tenantId + "/analytics/data/folder1/folder2")) {
            s3Service.createFolder(s3Bucket, tenantId + "/analytics/data/folder1/folder2");
        }
        StringInputStream sis = new StringInputStream("fileA1");
        s3Service.uploadInputStream(s3Bucket, tenantId + "/analytics/data/folder1/fileA.csv", sis, true);
        sis = new StringInputStream("fileA2");
        s3Service.uploadInputStream(s3Bucket, tenantId + "/analytics/data/folder1/folder2/fileA.csv", sis, true);
        sis = new StringInputStream("fileB");
        s3Service.uploadInputStream(s3Bucket, tenantId + "/analytics/data/folder1/folder2/fileB.csv", sis, true);

        protocol = importFromS3Service.getS3FsProtocol();
    }

    @AfterClass(groups = "functional")
    public void cleanUp() {
        s3Service.cleanupPrefix(s3Bucket, tenantId + "/");
    }

    @Test(groups = "functional")
    public void exploreS3FilePath() {
        String s3File = importFromS3Service.exploreS3FilePath(
                "/user/s-analytics/customers/ImportFromS3ServiceImplTestNG.ImportFromS3ServiceImplTestNG.Production/data/folder1/fileA.csv");
        Assert.assertEquals(s3File,
                protocol + "://" + s3Bucket + "/ImportFromS3ServiceImplTestNG/analytics/data/folder1/fileA.csv");

        s3File = importFromS3Service.exploreS3FilePath(
                "hdfs://localhost/user/s-analytics/customers/ImportFromS3ServiceImplTestNG.ImportFromS3ServiceImplTestNG.Production/data/folder1/fileA.csv");
        Assert.assertEquals(s3File,
                protocol + "://" + s3Bucket + "/ImportFromS3ServiceImplTestNG/analytics/data/folder1/fileA.csv");
    }

    @Test(groups = "functional")
    public void getFilesForDir() {
        final String filter = "file.*.csv";
        List<String> files = importFromS3Service.getFilesForDir(
                protocol + "://" + s3Bucket + "/ImportFromS3ServiceImplTestNG/analytics/data/folder1",
                filename -> {
                    String name = FilenameUtils.getName(filename);
                    return name.matches(filter);
                });
        Assert.assertEquals(files.size(), 3);
        Set<String> fileSet = new HashSet<>(files);
        Assert.assertTrue(fileSet
                .contains(protocol + "://" + s3Bucket + "/ImportFromS3ServiceImplTestNG/analytics/data/folder1/fileA.csv"));
        Assert.assertTrue(fileSet.contains(
                protocol + "://" + s3Bucket + "/ImportFromS3ServiceImplTestNG/analytics/data/folder1/folder2/fileA.csv"));
        Assert.assertTrue(fileSet.contains(
                protocol + "://" + s3Bucket + "/ImportFromS3ServiceImplTestNG/analytics/data/folder1/folder2/fileB.csv"));

    }

}
