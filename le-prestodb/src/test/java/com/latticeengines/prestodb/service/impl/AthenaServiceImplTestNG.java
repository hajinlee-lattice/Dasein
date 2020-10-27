package com.latticeengines.prestodb.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.AthenaDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.prestodb.exposed.service.AthenaService;
import com.latticeengines.prestodb.framework.PrestoDbFunctionalTestNGBase;

import reactor.core.publisher.Flux;

public class AthenaServiceImplTestNG extends PrestoDbFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AthenaServiceImplTestNG.class);

    @Inject
    private AthenaService athenaService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private S3Service s3Service;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    private String tableName;
    private String hdfsDir;
    private String s3Prefix;
    private String s3Dir;

    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        tableName = "athena_test_" + leStack.replace("-", "_");
        hdfsDir = "/tmp/athenaTest/" + leStack + "/input";
        s3Prefix = leStack + "/athenaTest/input";
        String s3Protocol = Boolean.TRUE.equals(useEmr) ? "s3a" : "s3n";
        s3Dir = s3Protocol + "://" + s3Bucket + "/" + s3Prefix;
    }

    @AfterClass(groups = "functional")
    public void tearDown() throws IOException {
        athenaService.deleteTableIfExists(tableName);
        if (HdfsUtils.fileExists(yarnConfiguration, s3Dir)) {
            HdfsUtils.rmdir(yarnConfiguration, s3Dir);
        }
        if (HdfsUtils.fileExists(yarnConfiguration, hdfsDir)) {
            HdfsUtils.rmdir(yarnConfiguration, hdfsDir);
        }
    }

    @Test(groups = "functional")
    public void testCrudTable() {
        uploadDataToS3(hdfsDir, s3Dir);
        athenaService.deleteTableIfExists(tableName);
        Assert.assertFalse(athenaService.tableExists(tableName));

        athenaService.createTableIfNotExists(tableName, s3Bucket, s3Prefix, DataUnit.DataFormat.AVRO);

        List<Map<String, Object>> lst = athenaService.query("SELECT * FROM " + tableName);
        Assert.assertEquals(lst.size(), 2);

        Flux<Map<String, Object>> flux = athenaService.queryFlux("SELECT * FROM " + tableName);
        List<Map<String, Object>> lst2 = flux.collectList().block();
        System.out.println(lst2);

        List<String> tableNames = athenaService.getTablesStartsWith("athena_test");
        Assert.assertTrue(tableNames.contains(tableName));
    }

    @Test(groups = "functional")
    public void testSaveDataUnit() throws IOException {
        testSaveDataUnit(DataUnit.DataFormat.AVRO, "Account", Collections.emptyList());
        testSaveDataUnit(DataUnit.DataFormat.PARQUET, "AccountExport", Collections.emptyList());

        List<Pair<String, String>> partitionKeys = Collections.singletonList(Pair.of("pk_date", "Date"));
        testSaveDataUnit(DataUnit.DataFormat.AVRO, "AccountPartition", partitionKeys);
        testSaveDataUnit(DataUnit.DataFormat.PARQUET, "AccountExportPartition", partitionKeys);
    }

    private void testSaveDataUnit(DataUnit.DataFormat format, String dataSet, List<Pair<String, String>> partitionKeys) throws IOException {
        String s3Root = leStack + "/athenaTest/data-unit/" + format.name().toLowerCase() + "/" + dataSet;
        if (s3Service.objectExist(s3Bucket, s3Root + "/")) {
            s3Service.cleanupDirectory(s3Bucket, s3Root);
        }
        String tableName = "AthenaTest_" + format + "_" + dataSet;
        athenaService.deleteTableIfExists(tableName);

        String dataRoot = "data/" + format.name().toLowerCase() + "/" + dataSet;
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        Resource[] resources = resolver.getResources(dataRoot + "/**/*");
        Pattern pattern = Pattern.compile(dataRoot + "/(?<relative>.*)$");
        log.info("Resolved resources for " + dataRoot);
        for (Resource resource : resources) {
            InputStream is;
            try {
                is = resource.getInputStream();
                String uri = resource.getURI().toString();
                if (uri.endsWith("_SUCCESS") || uri.endsWith(".avro") || uri.endsWith(".parquet")) {
                    Matcher matcher = pattern.matcher(uri);
                    String relativePath;
                    if (matcher.find()) {
                        relativePath = matcher.group("relative");
                    } else {
                        throw new IOException("Cannot parse relative path from uri " + uri);
                    }
                    s3Service.uploadInputStream(s3Bucket, s3Root + "/" + relativePath, is, true);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to get absolute path of resource file.", e);
            }
        }

        S3DataUnit s3DataUnit = new S3DataUnit();
        s3DataUnit.setTenant("AthenaTestTenant");
        s3DataUnit.setName(tableName);
        s3DataUnit.setBucket(s3Bucket);
        s3DataUnit.setPrefix(s3Root);
        s3DataUnit.setDataFormat(format);
        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            s3DataUnit.setTypedPartitionKeys(partitionKeys);
        }

        AthenaDataUnit athenaDataUnit = athenaService.saveDataUnit(s3DataUnit);
        System.out.println(JsonUtils.pprint(athenaDataUnit));
        Assert.assertNotNull(athenaDataUnit.getTenant());
        Assert.assertNotNull(athenaDataUnit.getName());
        String athenaTableName = athenaDataUnit.getAthenaTable();
        Assert.assertTrue(athenaService.tableExists(athenaTableName));
        Long cnt = athenaService.queryObject("SELECT COUNT(1) FROM " + athenaTableName, Long.class);
        Assert.assertEquals(cnt, athenaDataUnit.getCount());

        List<String> accountIds = athenaService.queryForList( //
                "SELECT AccountId FROM " + athenaTableName + " LIMIT 5", String.class);
        for (String accountId: accountIds) {
            Assert.assertNotNull(accountId);
        }

        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            Long partitionCnt = athenaService.queryObject( //
                    "SELECT COUNT(1) FROM " + athenaTableName + " WHERE month(pk_date) = 10", Long.class);
            Assert.assertTrue(partitionCnt > 0);
        }

        athenaService.deleteTableIfExists(athenaTableName);
        if (s3Service.objectExist(s3Bucket, s3Root + "/")) {
            s3Service.cleanupDirectory(s3Bucket, s3Root);
        }
    }

    private void uploadDataToS3(String hdfsDir, String s3Dir) {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Field1", String.class),
                Pair.of("Field2", Integer.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", 200},
                {"acc2", 211}
        };
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, hdfsDir)) {
                HdfsUtils.rmdir(yarnConfiguration, hdfsDir);
            }
            AvroUtils.uploadAvro(yarnConfiguration, data, fields, "record", hdfsDir);
        } catch (Exception e) {
            Assert.fail("Failed to upload test data to hdfs", e);
        }
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, s3Dir)) {
                HdfsUtils.rmdir(yarnConfiguration, s3Dir);
            }
            log.info("Copying from {} to {}", hdfsDir, s3Dir);
            HdfsUtils.copyFiles(yarnConfiguration, hdfsDir, s3Dir);
        } catch (Exception e) {
            Assert.fail("Failed to move test data to S3", e);
        }
    }


}
