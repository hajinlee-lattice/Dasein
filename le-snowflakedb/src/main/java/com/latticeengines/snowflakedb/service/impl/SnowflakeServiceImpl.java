package com.latticeengines.snowflakedb.service.impl;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.snowflakedb.exposed.service.SnowflakeService;
import com.latticeengines.snowflakedb.util.SnowflakeUtils;

@Component("snowflakeService")
public class SnowflakeServiceImpl implements SnowflakeService {

    private static final Log log = LogFactory.getLog(SnowflakeServiceImpl.class);

    private static final String AVRO_STAGE = "avro_stage";

    @Autowired
    @Qualifier(value = "snowflakeJdbcTemplate")
    private JdbcTemplate snowflakeJdbcTemplate;

    @Value("${aws.default.access.key.encrypted}")
    private String awsAccessKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String awsSecretKey;

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${common.le.stack}")
    private String leStack;

    @Override
    public void createDatabase(String db, String s3Bucket) {
        log.info("Creating database [" + db + "]");
        snowflakeJdbcTemplate.execute("CREATE DATABASE " + db);
        addAvroStage(db, s3Bucket);
    }

    @Override
    public void dropDatabaseIfExists(String db) {
        log.info("Dropping database [" + db + "]");
        snowflakeJdbcTemplate.execute("DROP DATABASE IF EXISTS " + db);
    }

    @Override
    public void createAvroTable(String db, String table, Schema schema) {
        createAvroTable(db, table, schema, null);
    }

    @Override
    public void createAvroTable(String db, String table, Schema schema, List<String> columnsToExpose) {
        log.info("Creating AVRO table [" + table + "] in [" + db + "]");

        // create the single column json table
        snowflakeJdbcTemplate.execute(String.format("CREATE OR REPLACE TABLE %s (%s VARIANT);\n",
                SnowflakeUtils.toQualified(db, SnowflakeUtils.toAvroRawTable(table)), SnowflakeUtils.AVRO_COLUMN));

        // create a view on top of it
        String view = SnowflakeUtils.schemaToView(schema, columnsToExpose);
        StringBuilder sb = new StringBuilder() //
                .append(String.format("CREATE OR REPLACE VIEW %s AS SELECT\n", SnowflakeUtils.toQualified(db, table))) //
                .append(view) //
                .append(String.format("FROM %s; \n",
                        SnowflakeUtils.toQualified(db, SnowflakeUtils.toAvroRawTable(table))));
        snowflakeJdbcTemplate.execute(sb.toString());
    }

    @Override
    public void loadAvroTableFromS3(String db, String table, String s3Folder) {
        String sql = String.format("COPY INTO %s \n" +
                "from @%s/%s/\n" +
                "pattern = '.*.avro'\n" +
                "on_error = 'continue'",
                SnowflakeUtils.toQualified(db, SnowflakeUtils.toAvroRawTable(table)),
                SnowflakeUtils.toQualified(db, AVRO_STAGE),
                s3Folder);
        snowflakeJdbcTemplate.execute(sql);
    }

    @Override
    public String s3PrefixForAvroStage() {
        return AVRO_STAGE;
    }

    private void addAvroStage(String db, String s3Bucket) {
        log.info("Adding S3 avro stage to database [" + db + "]");
        String stageName = SnowflakeUtils.toQualified(db, AVRO_STAGE);
        String region = awsRegion.replace("-", "_").toUpperCase();
        String url = String.format("s3://%s/%s/", s3Bucket, absoluteS3PrefixForAvroStage());

        String sql = String.format("CREATE OR REPLACE STAGE %s\n" +
                "  URL = '%s'\n" +
                "  CREDENTIALS = (AWS_KEY_ID='%s' AWS_SECRET_KEY='%s') \n" +
                "  REGION = '%s' \n" +
                "  FILE_FORMAT = ( TYPE='AVRO') \n", stageName, url, awsAccessKey, awsSecretKey, region);

        snowflakeJdbcTemplate.execute(sql);
    }

    private String absoluteS3PrefixForAvroStage() {
        if (StringUtils.isEmpty(leStack)) {
            return AVRO_STAGE;
        } else {
            return leStack + "/" + AVRO_STAGE;
        }
    }

}
