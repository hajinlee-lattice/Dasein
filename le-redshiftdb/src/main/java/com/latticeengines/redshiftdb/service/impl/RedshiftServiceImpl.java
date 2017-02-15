package com.latticeengines.redshiftdb.service.impl;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;

@Component("redshiftService")
public class RedshiftServiceImpl implements RedshiftService {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(RedshiftServiceImpl.class);

    @Autowired
    @Qualifier(value = "redshiftJdbcTemplate")
    private JdbcTemplate redshiftJdbcTemplate;

    @Value("${aws.default.access.key.encrypted}")
    private String awsAccessKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String awsSecretKey;

    @Value("${aws.region}")
    private String awsRegion;

    @Override
    public void createTable(String tableName, Schema schema) {
        try {
            redshiftJdbcTemplate.execute(RedshiftUtils.getCreateTableStatement(tableName, schema));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not create table %s in Redshift", tableName), e);
        }
    }

    @Override
    public void loadTableFromAvroInS3(String tableName, String s3bucket, String avroS3Prefix, String jsonPathS3Prefix) {
        String statement = "COPY %s\n" //
                + "FROM '%s'\n" //
                + "CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'\n" //
                + "FORMAT AVRO '%s'";
        statement = String.format(statement, tableName, getS3Path(s3bucket, avroS3Prefix), awsAccessKey, awsSecretKey,
                getS3Path(s3bucket, jsonPathS3Prefix));
        try {
            redshiftJdbcTemplate.execute(statement);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not copy table %s to Redshift from avro in s3", tableName),
                    e);
        }
    }

    @Override
    public void dropTable(String tableName) {
        try {
            redshiftJdbcTemplate.execute(String.format("DROP TABLE IF EXISTS %s", tableName));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not drop table %s in Redshift", tableName), e);
        }
    }

    private String getS3Path(String s3bucket, String s3prefix) {
        return String.format("s3://%s/%s", s3bucket, s3prefix);
    }

}
