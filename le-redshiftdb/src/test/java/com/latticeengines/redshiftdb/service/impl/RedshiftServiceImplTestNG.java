package com.latticeengines.redshiftdb.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.DistStyle;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.SortKeyType;
import com.latticeengines.domain.exposed.redshift.RedshiftUnloadParams;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-redshiftdb-context.xml" })
public class RedshiftServiceImplTestNG extends AbstractTestNGSpringContextTests {

    private static final String TABLE_NAME = "RedshiftServiceImplTestNG_EventTable";
    private static final String STAGING_TABLE_NAME = TABLE_NAME + "_staging";
    @Autowired
    private RedshiftService redshiftService;

    @Autowired
    private S3Service s3Service;

    @Autowired
    @Qualifier("redshiftJdbcTemplate")
    private JdbcTemplate redshiftJdbcTemplate;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @Value("${common.le.stack}")
    private String leStack;

    private String avroPrefix;
    private String jsonPathPrefix;
    private String unloadPrefix;
    private Schema schema;

    @BeforeClass(groups = "functional")
    public void setup() {
        avroPrefix = RedshiftUtils.AVRO_STAGE + "/" + leStack + "/eventTable/EventTable.avro";
        jsonPathPrefix = RedshiftUtils.AVRO_STAGE + "/" + leStack + "/eventTable/EventTable.jsonpath";
        unloadPrefix = RedshiftUtils.CSV_STAGE + "/" + leStack + "/eventTable";
        cleanupS3();
        cleanupRedshift();
    }

    @AfterClass(groups = "functional")
    public void tearDown() {
        cleanupS3();
    }

    private void cleanupS3() {
        s3Service.cleanupPrefix(s3Bucket, jsonPathPrefix);
        s3Service.cleanupPrefix(s3Bucket, avroPrefix);
        s3Service.cleanupPrefix(s3Bucket, unloadPrefix);
    }

    private void cleanupRedshift() {
        redshiftService.dropTable(TABLE_NAME);
        redshiftService.dropTable(STAGING_TABLE_NAME);
    }

    @Test(groups = "functional")
    public void loadDataToS3() throws URISyntaxException {
        InputStream avroStream = ClassLoader.getSystemResourceAsStream("avro/EventTable.avro");
        schema = AvroUtils.getSchema(new File(ClassLoader.getSystemResource("avro/EventTable.avro").toURI()));
        s3Service.uploadInputStream(s3Bucket, avroPrefix, avroStream, true);

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            RedshiftUtils.generateJsonPathsFile(schema, outputStream);
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
                s3Service.uploadInputStream(s3Bucket, jsonPathPrefix, inputStream, true);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test(groups = "functional", dependsOnMethods = "loadDataToS3")
    public void loadDataToRedshift() {
        RedshiftTableConfiguration redshiftTableConfig = new RedshiftTableConfiguration();
        redshiftTableConfig.setTableName(TABLE_NAME);
        redshiftTableConfig.setDistStyle(DistStyle.Key);
        redshiftTableConfig.setDistKey("lastmodifieddate");
        redshiftTableConfig.setSortKeyType(SortKeyType.Compound);
        redshiftTableConfig.setSortKeys(Arrays.asList("id"));
        redshiftTableConfig.setS3Bucket(s3Bucket);
        redshiftService.createTable(redshiftTableConfig, schema);
        redshiftService.loadTableFromAvroInS3(TABLE_NAME, s3Bucket, avroPrefix, jsonPathPrefix);
        redshiftService.vacuumTable(TABLE_NAME);
        redshiftService.analyzeTable(TABLE_NAME);
    }

    @Test(groups = "functional", dependsOnMethods = "loadDataToRedshift")
    public void unloadQueryToS3() {
        redshiftService.unloadTable(TABLE_NAME, s3Bucket, unloadPrefix, new RedshiftUnloadParams());
    }

    @Test(groups = "functional", dependsOnMethods = "loadDataToRedshift", enabled = false)
    public void queryTableSchema() {
        List<Map<String, Object>> result = redshiftJdbcTemplate.queryForList(String.format(
                "SELECT \"COLUMN\", TYPE, ENCODING, DISTKEY, SORTKEY FROM PG_TABLE_DEF WHERE TABLENAME = '%s';",
                TABLE_NAME.toLowerCase()));
        assertEquals(result.size(), 23);
        for (Map<String, Object> row : result) {
            if (row.get("column").equals("id")) {
                assertEquals(row.get("sortkey"), 1);
            } else {
                assertEquals(row.get("sortkey"), 0);
            }
            if (row.get("column").equals("lastmodifieddate")) {
                assertEquals(row.get("distkey"), Boolean.TRUE);
            } else {
                assertEquals(row.get("distkey"), Boolean.FALSE);
            }
        }
    }

    @Test(groups = "functional", dependsOnMethods = "queryTableSchema", enabled = false)
    public void queryTable() {
        List<Map<String, Object>> result = redshiftJdbcTemplate
                .queryForList(String.format("SELECT * FROM %s LIMIT 10", TABLE_NAME));
        assertEquals(result.size(), 10);
        for (Map<String, Object> row : result) {
            assertNotNull(row.get("Id"));
        }
    }

    @Test(groups = "functional", dependsOnMethods = "queryTable", enabled = false)
    public void updateExistingRows() {
        redshiftService.createStagingTable(STAGING_TABLE_NAME, TABLE_NAME);
        redshiftService.loadTableFromAvroInS3(STAGING_TABLE_NAME, s3Bucket, avroPrefix, jsonPathPrefix);
        redshiftService.updateExistingRowsFromStagingTable(STAGING_TABLE_NAME, TABLE_NAME, "Id");
        redshiftService.dropTable(STAGING_TABLE_NAME);
        List<Map<String, Object>> result = redshiftJdbcTemplate
                .queryForList(String.format("SELECT * FROM %s LIMIT 10", TABLE_NAME));
        assertEquals(result.size(), 10);
        for (Map<String, Object> row : result) {
            assertNotNull(row.get("Id"));
        }
    }

    @Test(groups = "functional", dependsOnMethods = "queryTable", enabled = false)
    public void getTables() {
        List<String> tables = redshiftService.getTables("");
        Assert.assertTrue(tables.contains(TABLE_NAME.toLowerCase()));
        tables = redshiftService.getTables(TABLE_NAME.split("_")[0]);
        Assert.assertTrue(tables.contains(TABLE_NAME.toLowerCase()));
    }
}
