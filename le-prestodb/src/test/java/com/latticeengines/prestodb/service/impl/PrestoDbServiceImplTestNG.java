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
import javax.sql.DataSource;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.PrestoDataUnit;
import com.latticeengines.prestodb.exposed.service.PrestoConnectionService;
import com.latticeengines.prestodb.exposed.service.PrestoDbService;
import com.latticeengines.prestodb.framework.PrestoDbFunctionalTestNGBase;

public class PrestoDbServiceImplTestNG extends PrestoDbFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PrestoDbServiceImplTestNG.class);

    @Inject
    private PrestoDbService prestoDbService;

    @Inject
    private PrestoConnectionService prestoConnectionService;

    @Inject
    private Configuration yarnConfiguration;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${prestodb.partition.avro.enabled}")
    private boolean enableAvroPartition;

    @Value("${prestodb.partition.parquet.enabled}")
    private boolean enableParquetPartition;

    private JdbcTemplate jdbcTemplate;

    @BeforeClass(groups = "functional")
    public void setup() throws IOException {
        DataSource dataSource = prestoConnectionService.getPrestoDataSource();
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Test(groups = "functional")
    public void testConnection() {
        String avroDir = "/tmp/prestoTest/" + leStack + "/input";
        String tableName = "TestTable";
        try {
            prestoDbService.deleteTableIfExists(tableName);
            if (HdfsUtils.fileExists(yarnConfiguration, avroDir)) {
                HdfsUtils.rmdir(yarnConfiguration, avroDir);
            }
        } catch (IOException e) {
            Assert.fail("Failed to clean up hdfs", e);
        }

        Assert.assertFalse(prestoDbService.tableExists(tableName));

        uploadDataToHdfs(avroDir);
        prestoDbService.createTableIfNotExists(tableName, avroDir);
        SleepUtils.sleep(500);

        Assert.assertTrue(prestoDbService.tableExists(tableName));
        List<Map<String, Object>> lst = jdbcTemplate.queryForList("SELECT * FROM " + tableName);
        Assert.assertEquals(lst.size(), 2);
    }

    @Test(groups = "functional")
    public void testSaveDataUnit() throws IOException {
        testSaveDataUnit(DataUnit.DataFormat.AVRO, "Account", Collections.emptyList());
        testSaveDataUnit(DataUnit.DataFormat.PARQUET, "AccountExport", Collections.emptyList());

        List<Pair<String, String>> partitionKeys = Collections.singletonList(Pair.of("pk_date", "Date"));
        testSaveDataUnit(DataUnit.DataFormat.PARQUET, "AccountExportPartition", partitionKeys);
        testSaveDataUnit(DataUnit.DataFormat.AVRO, "AccountPartition", partitionKeys);
    }

    private void testSaveDataUnit(DataUnit.DataFormat format, String dataSet, List<Pair<String, String>> partitionKeys) throws IOException {
        String hdfsRoot = "/tmp/prestoTest/" + leStack + "/data-unit/" + format.name().toLowerCase() + "/" + dataSet;
        if (HdfsUtils.fileExists(yarnConfiguration, hdfsRoot)) {
            HdfsUtils.rmdir(yarnConfiguration, hdfsRoot);
        }
        String tableName = "PrestoTest_" + format + "_" + dataSet;
        prestoDbService.deleteTableIfExists(tableName);

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
                    HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is, hdfsRoot + "/" + relativePath);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to get absolute path of resource file.", e);
            }
        }
        HdfsDataUnit hdfsDataUnit = new HdfsDataUnit();
        hdfsDataUnit.setTenant("PrestoTestTenant");
        hdfsDataUnit.setName(tableName);
        hdfsDataUnit.setPath(hdfsRoot);
        hdfsDataUnit.setDataFormat(format);
        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            hdfsDataUnit.setTypedPartitionKeys(partitionKeys);
        }

        PrestoDataUnit prestoDataUnit = prestoDbService.saveDataUnit(hdfsDataUnit);
        System.out.println(JsonUtils.pprint(prestoDataUnit));
        String clusterId = prestoConnectionService.getClusterId();
        Assert.assertTrue(prestoDbService.tableExists(prestoDataUnit.getPrestoTableName(clusterId)));
        Long cnt = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM " + tableName, Long.class);
        Assert.assertEquals(cnt, prestoDataUnit.getCount());

        List<String> accountIds = jdbcTemplate.queryForList( //
                "SELECT AccountId FROM " + tableName + " LIMIT 5", String.class);
        for (String accountId: accountIds) {
            Assert.assertNotNull(accountId);
        }

        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            if ((DataUnit.DataFormat.PARQUET.equals(format) && enableParquetPartition) || //
                    (DataUnit.DataFormat.AVRO.equals(format) && enableAvroPartition)) {
                Long partitionCnt = jdbcTemplate.queryForObject( //
                        "SELECT COUNT(1) FROM " + tableName + " WHERE month(pk_date) = 10", Long.class);
                Assert.assertTrue(partitionCnt > 0);
            }
        }

    }

    private void uploadDataToHdfs(String avroDir) {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Field1", String.class),
                Pair.of("Field2", Integer.class)
        );
        Object[][] data = new Object[][]{
                {"acc1", 200},
                {"acc2", 211}
        };
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, avroDir)) {
                HdfsUtils.rmdir(yarnConfiguration, avroDir);
            }
            AvroUtils.uploadAvro(yarnConfiguration, data, fields, "record", avroDir);
        } catch (Exception e) {
            Assert.fail("Failed to upload test data to hdfs", e);
        }
    }

}
