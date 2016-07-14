package com.latticeengines.snowflakedb.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.snowflakedb.exposed.service.SnowflakeService;
import com.latticeengines.snowflakedb.exposed.util.SnowflakeUtils;


@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-snowflake-context.xml" })
public class SnowflakeServiceImplTestNG extends AbstractTestNGSpringContextTests {

    private static final String DB_NAME = "SnowflakeServiceImplTest";

    @Autowired
    private SnowflakeService snowflakeService;

    @Autowired
    private S3Service s3Service;

    @Autowired
    @Qualifier("snowflakeJdbcTemplate")
    private JdbcTemplate snowflakeJdbcTemplate;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @Value("${common.le.stack}")
    private String leStack;

    private String dbName;

    @BeforeClass(groups = "functional")
    public void setup() {
        dbName = leStack + "_" + DB_NAME;
    }

    @BeforeMethod(groups = "functional")
    public void setupMethod() {
        snowflakeService.dropDatabaseIfExists(dbName);
    }

    @AfterMethod(groups = "functional")
    public void teardownMethod() {
        snowflakeService.dropDatabaseIfExists(dbName);
    }

    @Test(groups = "functional")
    public void testCreateAndDeleteDatabase() {
        Assert.assertFalse(verityDatabaseExists(dbName), dbName + "should be deleted");
        snowflakeService.createDatabase(dbName, s3Bucket);
        Assert.assertTrue(verityDatabaseExists(dbName), dbName + "should be created");
        snowflakeService.dropDatabaseIfExists(dbName);
        Assert.assertFalse(verityDatabaseExists(dbName), dbName + "should be deleted");
    }

    @Test(groups = "functional")
    public void testCreateAndDeleteAvroTable() {
        snowflakeService.createDatabase(dbName, s3Bucket);
        String table = "AvroTable";

        Schema schema = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"" + table + "\",\"doc\":\"Testing data\"," + "\"fields\":[" //
                        + "{\"name\":\"Field1\",\"type\":\"int\"}," //
                        + "{\"name\":\"Field2\",\"type\":\"string\"}," //
                        + "{\"name\":\"Field3\",\"type\":[\"string\",\"null\"]}," //
                        + "{\"name\":\"Field4\",\"type\":[\"int\",\"null\"]}]}");

        snowflakeService.createAvroTable(dbName, table, schema, Arrays.asList("field1", "field2", "field3"));

        snowflakeJdbcTemplate.execute(String.format(
                "INSERT INTO %s SELECT "
                        + "OBJECT_CONSTRUCT('Field1', 1, 'Field2', 'hello', 'Field3', PARSE_JSON('NULL'), 'Field4', 4)",
                SnowflakeUtils.toQualified(dbName, SnowflakeUtils.toAvroRawTable(table))));

        Map<String, Object> row = snowflakeJdbcTemplate
                .queryForMap(String.format("SELECT * FROM %s limit 1;", SnowflakeUtils.toQualified(dbName, table)));

        Assert.assertEquals(Integer.valueOf(row.get("FIELD1").toString()), new Integer(1));
        Assert.assertEquals(row.get("FIELD2"), "hello");
        Assert.assertEquals(row.get("FIELD3").toString(), "null");
        Assert.assertFalse(row.containsKey("FIELD4"));
    }

    @Test(groups = "functional")
    public void testLoadAvroTableFromStage() throws IOException {
        snowflakeService.createDatabase(dbName, s3Bucket);
        String table = "AvroTable2";
        InputStream avroStream = ClassLoader.getSystemResourceAsStream("avro/hgdata.avro");
        Schema schema = AvroUtils.readSchemaFromInputStream(avroStream);
        snowflakeService.createAvroTable(dbName, table, schema, Arrays.asList("Domain", "Creation_Date"));
        uploadAvroToS3();
        snowflakeService.loadAvroTableFromS3(dbName, table, leStack + "/hgdata");

        Map<String, Object> row = snowflakeJdbcTemplate
                .queryForMap(String.format("SELECT * FROM %s limit 1;", SnowflakeUtils.toQualified(dbName, table)));
        Assert.assertTrue(row.containsKey("Domain"));
        Assert.assertFalse(row.containsKey("Supplier_Name"));

        cleanupS3();
    }

    private boolean verityDatabaseExists(String databaseName) {
        List<Map<String, Object>> results = snowflakeJdbcTemplate.queryForList("SHOW DATABASES");
        for (Map<String, Object> row : results) {
            String dbName = (String) row.get("name");
            if (dbName.equalsIgnoreCase(databaseName)) {
                return true;
            }
        }
        return false;
    }

    private void uploadAvroToS3() {
        InputStream avroStream = ClassLoader.getSystemResourceAsStream("avro/hgdata.avro");
        String key = SnowflakeUtils.AVRO_STAGE + "/" + leStack + "/hgdata/part-1.avro";
        s3Service.uploadInputStream(s3Bucket, key, avroStream, true);
    }

    private void cleanupS3() {
        String prefix = SnowflakeUtils.AVRO_STAGE + "/" + leStack + "/hgdata";
        s3Service.cleanupPrefix(s3Bucket, prefix);
    }
}
