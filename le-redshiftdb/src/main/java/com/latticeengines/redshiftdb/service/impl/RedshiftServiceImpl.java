package com.latticeengines.redshiftdb.service.impl;

import java.sql.SQLType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.retry.support.RetryTemplate;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftUnloadParams;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;

public class RedshiftServiceImpl implements RedshiftService {

    private static final Logger log = LoggerFactory.getLogger(RedshiftServiceImpl.class);

    private final String awsAccessKey;
    private final String awsSecretKey;
    private final String parition;
    private final JdbcTemplate redshiftJdbcTemplate;

    RedshiftServiceImpl(String awsAccessKey, String awsSecretKey, String partition, JdbcTemplate redshiftJdbcTemplate) {
        this.awsAccessKey = awsAccessKey;
        this.awsSecretKey = awsSecretKey;
        this.parition = partition;
        this.redshiftJdbcTemplate = redshiftJdbcTemplate;
    }

    @Override
    public void createTable(RedshiftTableConfiguration redshiftTableConfig, Schema schema) {
        try {
            log.info("Creating redshift table {} in partition {}", redshiftTableConfig.getTableName(), parition);
            redshiftJdbcTemplate.execute(RedshiftUtils.getCreateTableStatement(redshiftTableConfig, schema));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not create table %s in partition %s",
                            redshiftTableConfig.getTableName(), parition), e);
        }
    }

    @Override
    public void insertValuesIntoTable(String tableName, List<Pair<String, Class<?>>> schema, List<List<Object>> data) {
        List<String> fields = schema.stream().map(Pair::getLeft).collect(Collectors.toList());
        List<SQLType> sqlTypes = schema.stream().map(Pair::getRight).map(AvroUtils::getSqlType)
                .collect(Collectors.toList());
        int totoRows = data.size();
        int totalCardinality = 5000;
        int pageSize = Math.max(totalCardinality / sqlTypes.size(), 1);
        int inserted = 0;
        while (inserted < totoRows) {
            List<List<Object>> page = data.subList(inserted, Math.min(inserted + pageSize, totoRows));
            String sql = RedshiftUtils.insertValuesIntoTableStatement(tableName, fields, page.size());
            redshiftJdbcTemplate.execute(sql, (PreparedStatementCallback<Boolean>) ps -> {
                int paramIdx = 1;
                for (List<Object> row : page) {
                    for (int i = 0; i < row.size(); i++) {
                        Object val = row.get(i);
                        SQLType sqlType = sqlTypes.get(i);
                        ps.setObject(paramIdx++, val, sqlType);
                    }
                }
                return ps.execute();
            });
            inserted += page.size();
        }
    }

    @Override
    public void loadTableFromAvroInS3(String tableName, String s3bucket, String avroS3Prefix, String jsonPathS3Prefix) {
        log.info(String.format("Loading date into %s : %s from S3 bucket %s/%s", parition, tableName, s3bucket, avroS3Prefix));
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
            throw new RuntimeException(
                    String.format("Could not copy table %s to partition %s from avro in s3", tableName, parition), e);
        }
    }

    @Override
    public void dropTable(String tableName) {
        try {
            log.info("Dropping redshift table {} in partition {}", tableName, parition);
            redshiftJdbcTemplate.execute(RedshiftUtils.dropTableStatement(tableName));
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Could not drop table %s in partition %s", tableName, parition), e);
        }
    }

    @Override
    public void createStagingTable(String stageTableName, String targetTableName) {
        try {
            dropTable(stageTableName);
            log.info("Creating staging redshift table {} for target table {} in partition {}",
                    stageTableName, targetTableName, parition);
            redshiftJdbcTemplate.execute(RedshiftUtils.createStagingTableStatement(stageTableName, targetTableName));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Could not create stage table %s in partition %s",
                    stageTableName, parition), e);
        }
    }

    @Override
    public void renameTable(String originalTableName, String newTableName) {
        try {
            log.info("Renaming redshift table {} to {}} in partition {}", originalTableName, newTableName, parition);
            redshiftJdbcTemplate.execute(RedshiftUtils.renameTableStatement(originalTableName, newTableName));
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Could not alter table %s to %s in partition %s", //
                            originalTableName, newTableName, parition), e);
        }
    }

    @Override
    public void updateExistingRowsFromStagingTable(String stageTableName, String targetTableName,
            String... joinFields) {
        try {
            log.info(String.format("Inserting %s : %s using %s", parition, targetTableName, stageTableName));
            StringBuffer sb = new StringBuffer();
            sb.append("BEGIN TRANSACTION;");
            sb.append(RedshiftUtils.updateExistingRowsFromStagingTableStatement(stageTableName, targetTableName,
                    joinFields));
            sb.append("END TRANSACTION;");
            redshiftJdbcTemplate.execute(sb.toString());
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Could not update table %s in partition %s", targetTableName, parition), e);
        }
    }

    @Override
    public void replaceTable(String stageTableName, String targetTableName) {
        try {
            log.info(String.format("Replacing %s with %s in partition %s", targetTableName, stageTableName, parition));
            StringBuffer sb = new StringBuffer();
            sb.append("BEGIN TRANSACTION;");
            sb.append(RedshiftUtils.dropTableStatement(targetTableName));
            sb.append(RedshiftUtils.renameTableStatement(stageTableName, targetTableName));
            sb.append("END TRANSACTION;");
            redshiftJdbcTemplate.execute(sb.toString());
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format("Could not replace table %s in partition %s", targetTableName, parition), e);
        }
    }

    private String getS3Path(String s3bucket, String s3prefix) {
        return String.format("s3://%s/%s", s3bucket, s3prefix);
    }

    @Override
    public void analyzeTable(String tableName) {
        log.info("Analyze table {} in partition {}", tableName, parition);
        redshiftJdbcTemplate.execute(String.format("ANALYZE %s", tableName));
    }

    @Override
    public void vacuumTable(String tableName) {
        log.info("Vacuum table {} in partition {}", tableName, parition);
        redshiftJdbcTemplate.execute(String
                .format("SET wlm_query_slot_count to 4; VACUUM FULL %s; SET wlm_query_slot_count to 1;", tableName));
    }

    @Override
    public Long countTable(String tableName) {
        log.info("Count table {} in partition {}", tableName, parition);
        return redshiftJdbcTemplate.queryForObject(String.format("SELECT COUNT(1) FROM %s", tableName), Long.class);
    }

    @Override
    public void cloneTable(String srcTable, String tgtTable) {
        log.info("Clone table {} to {} in partition {}", srcTable, tgtTable, parition);
        RetryTemplate retry = RetryUtils.getRetryTemplate(3);
        try {
            retry.execute(context -> {
                if (context.getRetryCount() >= 1) {
                    log.warn("Last clone ({} -> {}) wasn't successful. Retrying for {} times", srcTable, tgtTable,
                            context.getRetryCount());
                }
                redshiftJdbcTemplate.execute(String.format("DROP TABLE IF EXISTS %s", tgtTable));
                redshiftJdbcTemplate.execute(String.format("CREATE TABLE %s (LIKE %s)", tgtTable, srcTable));
                redshiftJdbcTemplate.execute(String.format("INSERT INTO %s (SELECT * FROM %s)", tgtTable, srcTable));
                Long srcCnt = countTable(srcTable);
                Long tgtCnt = countTable(tgtTable);
                if (srcCnt.longValue() != tgtCnt.longValue()) {
                    log.error(
                            "Target table {} has total row count as {}, different with source table {} total row count {}",
                            tgtTable, tgtCnt, srcTable, srcCnt);
                    throw new RuntimeException(String.format("Table clone from %s to %s failed", srcTable, tgtTable));
                }
                return null;
            });
        } catch (Exception ex) {
            throw ex;
        }
    }

    @Override
    public boolean hasTable(String tableName) {
        String sql = "SELECT tablename FROM pg_table_def WHERE schemaname = 'public'" //
                + " AND tablename = '" + tableName.toLowerCase() + "';";
        List<Map<String, Object>> results = redshiftJdbcTemplate.queryForList(sql);
        return CollectionUtils.isNotEmpty(results);
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

    @Override
    public void unloadTable(String tableName, String s3bucket, String s3Prefix, RedshiftUnloadParams unloader) {
        String s3Path = String.format("s3://%s/%s/", s3bucket, s3Prefix);
        String creds = "CREDENTIALS 'aws_access_key_id=" + awsAccessKey + ";aws_secret_access_key=" + awsSecretKey + "'";
        String sql = RedshiftUtils.unloadTableStatement(tableName, s3Path, creds, unloader);
        redshiftJdbcTemplate.execute(sql);
    }

}
