package com.latticeengines.metadata.service.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.DataUnitCrossTenantService;

public class DataUnitCrossTenantServiceImplTestNG extends MetadataFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DataUnitCrossTenantServiceImplTestNG.class);

    @Inject
    private DataUnitCrossTenantService dataUnitCrossTenantService;

    @Inject
    private S3Service s3Service;
    @Autowired
    private Configuration yarnConfiguration;
    @Inject
    private DynamoService dynamoService;

    @Value("${common.le.environment}")
    private String env;

    @Value("${common.le.stack}")
    private String stack;

    @Value("${datacloud.collection.s3bucket}")
    private String s3Bucket;

    private static final String S3FOLDERNAME = "testS3";
    private static final String PARTITION_KEY = "PartitionId";
    private static final String SORT_KEY = "SortId";

    private String testTenantId;
    private String customerSpace;
    private String dynamoTablename;
    private String s3Key;
    private String hdfsPath;

    @BeforeClass(groups = "functional")
    public void setup() {
        functionalTestBed.bootstrap(1);
        Tenant testTenant = functionalTestBed.getMainTestTenant();
        customerSpace = CustomerSpace.parse(testTenant.getId()).toString();
        testTenantId = CustomerSpace.shortenCustomerSpace(customerSpace);
    }

    @Test(groups = "functional")
    public void testCleanupByTenant() throws InterruptedException {
        prepareTestData();
        Thread.sleep(2000L);
        Assert.assertTrue(dataUnitCrossTenantService.cleanupByTenant(customerSpace));
        Assert.assertFalse(dynamoService.hasTable(dynamoTablename));
        Assert.assertTrue(CollectionUtils.isEmpty(s3Service.getFilesForDir(s3Bucket, s3Key)));
        boolean fileExisted = true;
        try {
            fileExisted = HdfsUtils.fileExists(yarnConfiguration, hdfsPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Assert.assertFalse(fileExisted);
    }

    private void prepareTestData() {
        prepareTestDataForS3();
        prepareTestDataForHdfs();
        prepareTestDataForDynamo();
    }

    private void prepareTestDataForS3() {
        String ingestionDir = S3PathBuilder.getUiDisplayS3Dir(s3Bucket, "tests3",
                S3FOLDERNAME);
        log.info("ingestionDir is :" + ingestionDir);
        String content = "create test s3 data";
        InputStream is = new ByteArrayInputStream(content.getBytes());
        s3Key = ingestionDir + "testS3.txt";
        String linkedDir = "s3a://" + s3Bucket + "/" + ingestionDir;
        log.info("linkedDir is :" + linkedDir);
        s3Service.uploadInputStream(s3Bucket, s3Key, is, true);
        S3DataUnit s3DataUnit = new S3DataUnit();
        s3DataUnit.setTenant(testTenantId);
        s3DataUnit.setLinkedDir(linkedDir);
        s3DataUnit.setName("testS3");
        dataUnitCrossTenantService.createOrUpdateByNameAndStorageType(customerSpace, s3DataUnit);
    }

    private void prepareTestDataForHdfs() {
        String content = "create test hdfs data";
        InputStream is = new ByteArrayInputStream(content.getBytes());
        hdfsPath = testTenantId + "/tmp/hdfs2s3/testHdfs.txt";
        try {
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is, hdfsPath);
            HdfsDataUnit hdfsDataUnit = new HdfsDataUnit();
            hdfsDataUnit.setPath(hdfsPath);
            hdfsDataUnit.setTenant(testTenantId);
            hdfsDataUnit.setName("testHdfs");
            dataUnitCrossTenantService.createOrUpdateByNameAndStorageType(customerSpace, hdfsDataUnit);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void prepareTestDataForDynamo() {
        dynamoTablename = "DataUnitServiceImplDeploymentTestNG_" + env + "_" + stack;
        dynamoService.deleteTable(dynamoTablename);

        long readCapacityUnits = 10;
        long writeCapacityUnits = 10;
        String partitionKeyName = PARTITION_KEY;
        String partitionKeyType = ScalarAttributeType.S.name();
        String sortKeyName = SORT_KEY;
        String sortKeyType = ScalarAttributeType.S.name();

        dynamoService.createTable(dynamoTablename, readCapacityUnits, writeCapacityUnits, partitionKeyName, partitionKeyType,
                sortKeyName, sortKeyType);

        DynamoDataUnit dynamoDataUnit = new DynamoDataUnit();
        dynamoDataUnit.setLinkedTable(dynamoTablename);
        dynamoDataUnit.setLinkedTenant(testTenantId);
        dynamoDataUnit.setPartitionKey(partitionKeyName);
        dynamoDataUnit.setSortKey(sortKeyName);
        dynamoDataUnit.setSignature("0000");
        dynamoDataUnit.setName(dynamoTablename);
        dataUnitCrossTenantService.createOrUpdateByNameAndStorageType(customerSpace, dynamoDataUnit);
        log.info("Dynamo tableName is " + dynamoDataUnit.getName());
    }
}
