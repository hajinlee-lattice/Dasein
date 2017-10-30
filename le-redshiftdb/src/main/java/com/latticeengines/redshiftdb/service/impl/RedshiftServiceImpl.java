package com.latticeengines.redshiftdb.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(RedshiftServiceImpl.class);

    @Autowired
    @Qualifier(value = "redshiftJdbcTemplate")
    private JdbcTemplate redshiftJdbcTemplate;

    @Value("${aws.default.access.key}")
    private String awsAccessKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String awsSecretKey;

    @Value("${aws.region}")
    private String awsRegion;

    @Override
    public void createTable(RedshiftTableConfiguration redshiftTableConfig, Schema schema) {
        try {
            log.info("Creating redshift table " + redshiftTableConfig.getTableName());
            redshiftJdbcTemplate.execute(RedshiftUtils.getCreateTableStatement(redshiftTableConfig, schema));
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Could not create table %s in Redshift", redshiftTableConfig.getTableName()), e);
        }
    }

    @Override
    public void loadTableFromAvroInS3(String tableName, String s3bucket, String avroS3Prefix, String jsonPathS3Prefix) {
        log.info(String.format("Loading date into %s from S3 bucket %s/%s", tableName, s3bucket, avroS3Prefix));
        String statement = "COPY %s\n" //
                + "FROM '%s'\n" //
                + "CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'\n" //
                + "FORMAT AVRO '%s' " //
                + "EMPTYASNULL " //
                + "TRUNCATECOLUMNS " //
                + "COMPUPDATE OFF " //
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
            log.info("Dropping redshift table " + tableName);
            redshiftJdbcTemplate.execute(RedshiftUtils.dropTableStatement(tableName));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not drop table %s in Redshift", tableName), e);
        }
    }

    @Override
    public void createStagingTable(String stageTableName, String targetTableName) {
        try {
            log.info("Creating staging redshift table " + stageTableName + " for target table " + targetTableName);
            redshiftJdbcTemplate.execute(RedshiftUtils.createStagingTableStatement(stageTableName, targetTableName));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not create stage table %s in Redshift", stageTableName), e);
        }
    }

    @Override
    public void renameTable(String originalTableName, String newTableName) {
        try {
            log.info("Renaming redshift table " + originalTableName + " to " + newTableName);
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
            log.info(String.format("Inserting %s using %s", targetTableName, stageTableName));
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
            log.info(String.format("Replacing %s with %s", targetTableName, stageTableName));
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

    @Override
    public void analyzeTable(String tableName) {
        log.info("Analyze table " + tableName);
        redshiftJdbcTemplate.execute(String.format("ANALYZE %s", tableName));
    }

    @Override
    public void vacuumTable(String tableName) {
        log.info("Vacuum table " + tableName);
        redshiftJdbcTemplate.execute(String.format("SET wlm_query_slot_count to 4; VACUUM FULL %s; SET wlm_query_slot_count to 1;", tableName));
    }

    @Override
    public void cloneTable(String srcTable, String tgtTable) {
        log.info("Clone table " + srcTable + " to " + tgtTable);
        redshiftJdbcTemplate.execute(String.format("CREATE TABLE %s (LIKE %s)", tgtTable, srcTable));
        redshiftJdbcTemplate.execute(String.format("INSERT INTO %s (SELECT * FROM %s)", tgtTable, srcTable));
    }

    @Override
    public List<String> getTables(String prefix) {
        String sql = "SELECT DISTINCT tablename FROM pg_table_def WHERE schemaname = 'public'";
        if (StringUtils.isNotBlank(prefix)) {
            sql += " AND tablename LIKE '" + prefix.toLowerCase() + "%'";
        }
        sql += " ORDER BY tablename";
        List<Map<String, Object>> results = redshiftJdbcTemplate.queryForList(sql);
        if (results == null || results.isEmpty()) {
            return Collections.emptyList();
        } else {
            return results.stream().map(m -> (String) m.get("tablename")).collect(Collectors.toList());
        }
    }

}
