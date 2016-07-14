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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.snowflakedb.exposed.service.SnowflakeService;
import com.latticeengines.snowflakedb.util.SnowflakeUtils;


@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-snowflake-context.xml" })
public class SnowflakeServiceImplTestNG extends AbstractTestNGSpringContextTests {

    private static final String DB_NAME = "SnowflakeServiceImplTestNG";

    @Autowired
    private SnowflakeService snowflakeService;

    @Autowired
    private S3Service s3Service;

    @Autowired
    @Qualifier("snowflakeJdbcTemplate")
    private JdbcTemplate snowflakeJdbcTemplate;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @BeforeMethod(groups = "functional")
    public void setup() {
        snowflakeService.dropDatabaseIfExists(DB_NAME);
    }

    @AfterMethod(groups = "functional")
    public void teardown() {
        snowflakeService.dropDatabaseIfExists(DB_NAME);
    }

    @Test(groups = "functional")
    public void testCreateAndDeleteDatabase() {
        Assert.assertFalse(verityDatabaseExists(DB_NAME), DB_NAME + "should be deleted");
        snowflakeService.createDatabase(DB_NAME, s3Bucket);
        Assert.assertTrue(verityDatabaseExists(DB_NAME), DB_NAME + "should be created");
        snowflakeService.dropDatabaseIfExists(DB_NAME);
        Assert.assertFalse(verityDatabaseExists(DB_NAME), DB_NAME + "should be deleted");
    }

    @Test(groups = "functional")
    public void testCreateAndDeleteAvroTable() {
        snowflakeService.createDatabase(DB_NAME, s3Bucket);
        String table = "AvroTable";

        Schema schema = new Schema.Parser()
                .parse("{\"type\":\"record\",\"name\":\"" + table + "\",\"doc\":\"Testing data\"," + "\"fields\":[" //
                        + "{\"name\":\"Field1\",\"type\":\"int\"}," //
                        + "{\"name\":\"Field2\",\"type\":\"string\"}," //
                        + "{\"name\":\"Field3\",\"type\":[\"string\",\"null\"]}," //
                        + "{\"name\":\"Field4\",\"type\":[\"int\",\"null\"]}]}");

        snowflakeService.createAvroTable(DB_NAME, table, schema, Arrays.asList("field1", "field2", "field3"));

        snowflakeJdbcTemplate.execute(String.format(
                "INSERT INTO %s SELECT "
                        + "OBJECT_CONSTRUCT('Field1', 1, 'Field2', 'hello', 'Field3', PARSE_JSON('NULL'), 'Field4', 4)",
                SnowflakeUtils.toQualified(DB_NAME, SnowflakeUtils.toAvroRawTable(table))));

        Map<String, Object> row = snowflakeJdbcTemplate
                .queryForMap(String.format("SELECT * FROM %s limit 1;", SnowflakeUtils.toQualified(DB_NAME, table)));

        Assert.assertEquals(Integer.valueOf(row.get("FIELD1").toString()), new Integer(1));
        Assert.assertEquals(row.get("FIELD2"), "hello");
        Assert.assertEquals(row.get("FIELD3").toString(), "null");
        Assert.assertFalse(row.containsKey("FIELD4"));
    }

    @Test(groups = "functional")
    public void testLoadAvroTableFromStage() throws IOException {
        snowflakeService.createDatabase(DB_NAME, s3Bucket);
        String table = "AvroTable2";
        InputStream avroStream = ClassLoader.getSystemResourceAsStream("avro/feature.avro");
        Schema schema = AvroUtils.readSchemaFromInputStream(avroStream);
        snowflakeService.createAvroTable(DB_NAME, table, schema, Arrays.asList("Domain", "Creation_Date"));
        uploadAvroToS3();
        snowflakeService.loadAvroTableFromS3(DB_NAME, table, "feature");

        Map<String, Object> row = snowflakeJdbcTemplate
                .queryForMap(String.format("SELECT * FROM %s limit 1;", SnowflakeUtils.toQualified(DB_NAME, table)));
        Assert.assertTrue(row.containsKey("Domain"));
        Assert.assertFalse(row.containsKey("Supplier_Name"));
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
        InputStream avroStream = ClassLoader.getSystemResourceAsStream("avro/feature.avro");
        String key = snowflakeService.s3PrefixForAvroStage() + "/feature/part-1.avro";
        s3Service.uploadInputStream(s3Bucket, key, avroStream, true);
    }
}
