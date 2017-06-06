package com.latticeengines.redshiftdb.service.impl;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
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
    public void createTable(RedshiftTableConfiguration redshiftTableConfig, Schema schema) {
        try {
            redshiftJdbcTemplate.execute(RedshiftUtils.getCreateTableStatement(redshiftTableConfig, schema));
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Could not create table %s in Redshift", redshiftTableConfig.getTableName()), e);
        }
    }

    @Override
    public void loadTableFromAvroInS3(String tableName, String s3bucket, String avroS3Prefix, String jsonPathS3Prefix) {
        String statement = "COPY %s\n" //
                + "FROM '%s'\n" //
                + "CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'\n" //
                + "FORMAT AVRO '%s' " //
                + "EMPTYASNULL " //
                + "COMPUPDATE ON " //
                + "dateformat 'auto'";
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
            redshiftJdbcTemplate.execute(RedshiftUtils.dropTableStatement(tableName));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not drop table %s in Redshift", tableName), e);
        }
    }

    @Override
    public void createStagingTable(String stageTableName, String targetTableName) {
        try {
            redshiftJdbcTemplate.execute(RedshiftUtils.createStagingTableStatement(stageTableName, targetTableName));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not create stage table %s in Redshift", stageTableName), e);
        }
    }

    @Override
    public void renameTable(String originalTableName, String newTableName) {
        try {
            redshiftJdbcTemplate.execute(RedshiftUtils.renameTableStatement(originalTableName, newTableName));
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Could not alter table %s to %s in Redshift", originalTableName, newTableName), e);
        }
    }

    @Override
    public void updateExistingRowsFromStagingTable(String stageTableName, String targetTableName,
            String... joinFields) {
        try {
            StringBuffer sb = new StringBuffer();
            sb.append("BEGIN TRANSACTION;");
            sb.append(RedshiftUtils.updateExistingRowsFromStagingTableStatement(stageTableName, targetTableName,
                    joinFields));
            sb.append("END TRANSACTION;");
            redshiftJdbcTemplate.execute(sb.toString());
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not update table %s in Redshift", targetTableName), e);
        }
    }

    @Override
    public void replaceTable(String stageTableName, String targetTableName) {
        try {
            StringBuffer sb = new StringBuffer();
            sb.append("BEGIN TRANSACTION;");
            sb.append(RedshiftUtils.dropTableStatement(targetTableName));
            sb.append(RedshiftUtils.renameTableStatement(stageTableName, targetTableName));
            sb.append("END TRANSACTION;");
            redshiftJdbcTemplate.execute(sb.toString());
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not replace table %s in Redshift", targetTableName), e);
        }
    }

    private String getS3Path(String s3bucket, String s3prefix) {
        return String.format("s3://%s/%s", s3bucket, s3prefix);
    }

}
