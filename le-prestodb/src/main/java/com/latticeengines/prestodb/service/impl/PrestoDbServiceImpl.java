package com.latticeengines.prestodb.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;
import javax.sql.DataSource;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.ParquetUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.PrestoDataUnit;
import com.latticeengines.prestodb.exposed.service.PrestoConnectionService;
import com.latticeengines.prestodb.exposed.service.PrestoDbService;
import com.latticeengines.prestodb.util.PrestoUtils;

@Service
public class PrestoDbServiceImpl implements PrestoDbService {

    private static final Logger log = LoggerFactory.getLogger(PrestoDbServiceImpl.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private PrestoConnectionService prestoConnectionService;

    @Value("${prestodb.partition.avro.enabled}")
    private boolean enableAvroPartition;

    @Value("${prestodb.partition.parquet.enabled}")
    private boolean enableParquetPartition;

    private JdbcTemplate jdbcTemplate;

    public JdbcTemplate getJdbcTemplate() {
        if (jdbcTemplate == null) {
            DataSource dataSource = prestoConnectionService.getPrestoDataSource();
            jdbcTemplate = new JdbcTemplate(dataSource);
        }
        return jdbcTemplate;
    }

    @Override
    public boolean tableExists(String tableName) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        String checkStmt = PrestoUtils.getCheckTableStmt("hive", "default", tableName);
        return Boolean.TRUE.equals(jdbcTemplate.queryForObject(checkStmt, Boolean.class));
    }

    @Override
    public void deleteTableIfExists(String tableName) {
        String avscPath = getAvscPath(tableName);
        if (tableExists(tableName)) {
            JdbcTemplate jdbcTemplate = getJdbcTemplate();
            String createStmt = jdbcTemplate.queryForObject("SHOW CREATE TABLE " + tableName, String.class);
            DataUnit.DataFormat format = PrestoUtils.parseDataFormat(createStmt);
            if (DataUnit.DataFormat.AVRO.equals(format)) {
                avscPath = PrestoUtils.parseAvscPath(createStmt);
                try {
                    if (StringUtils.isNotBlank(avscPath) && !HdfsUtils.fileExists(yarnConfiguration, avscPath)) {
                        log.warn("Missing avsc file for {} at {}, writing a dummy one.", tableName, avscPath);
                        Schema schema = getDummySchema(tableName);
                        HdfsUtils.writeToFile(yarnConfiguration, avscPath, schema.toString());
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to upload avsc file to hdfs.", e);
                }
            }
            String deleteStmt = PrestoUtils.getDeleteTableStmt(tableName);
            jdbcTemplate.execute(deleteStmt);
        }

        try {
            if (StringUtils.isNotBlank(avscPath) && HdfsUtils.fileExists(yarnConfiguration, avscPath)) {
                log.info("Removing avsc file {}", avscPath);
                HdfsUtils.rmdir(yarnConfiguration, avscPath);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to remove avsc file from hdfs.", e);
        }
    }

    @Override
    public void createTableIfNotExists(String tableName, String dataDir, DataUnit.DataFormat format, //
                                       List<Pair<String, Class<?>>> partitionKeys) {
        if (DataUnit.DataFormat.AVRO.equals(format)) {
            createAvroTable(tableName, dataDir, partitionKeys);
        } else if (DataUnit.DataFormat.PARQUET.equals(format)) {
            createParquetTable(tableName, dataDir, partitionKeys);
        } else {
            throw new UnsupportedOperationException("Unknown data type " + format);
        }
    }

    private void createAvroTable(String tableName, String avroDir, List<Pair<String, Class<?>>> partitionKeys) {
        String avroGlob;
        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            avroGlob = PathUtils.toParquetOrAvroDir(avroDir) + "/**/*.avro";
        } else {
            avroGlob = PathUtils.toParquetOrAvroDir(avroDir) + "/*.avro";
        }
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        String avscPath = getAvscPath(tableName);
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, avscPath)) {
                HdfsUtils.rmdir(yarnConfiguration, avscPath);
            }
            HdfsUtils.writeToFile(yarnConfiguration, avscPath, schema.toString());
        } catch (IOException e) {
            throw new RuntimeException("Failed to upload avsc file to hdfs.", e);
        }
        List<Pair<String, Class<?>>> fields = AvroUtils.parseSchema(schema);
        if (avroDir.startsWith("/")) {
            String fs = yarnConfiguration.get("fs.defaultFS");
            avroDir = fs + avroDir;
            avscPath = fs + avscPath;
        }
        String createTableStmt;
        if (CollectionUtils.isNotEmpty(partitionKeys) && enableAvroPartition) {
            createTableStmt = PrestoUtils.getCreateAvroTableStmt(tableName, fields, partitionKeys, //
                    PathUtils.toParquetOrAvroDir(avroDir), avscPath);
        } else {
            createTableStmt = PrestoUtils.getCreateAvroTableStmt(tableName, fields, null, //
                    PathUtils.toParquetOrAvroDir(avroDir), avscPath);
        }
        log.info("Create table {} using avro dir {}.", tableName, avroDir);
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        if (fields.size() > 1000) {
            log.warn("There are {} columns in table {}, table creation is going to be slow", fields.size(), tableName);
        }
        jdbcTemplate.execute(createTableStmt);
        if (CollectionUtils.isNotEmpty(partitionKeys) && enableAvroPartition) {
            String syncPartitionStmt = PrestoUtils.getSyncPartitionStmt("default", tableName);
            jdbcTemplate.execute(syncPartitionStmt);
        }
    }

    private void createParquetTable(String tableName, String parquetDir, List<Pair<String, Class<?>>> partitionKeys) {
        String parquetGlob;
        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            parquetGlob = PathUtils.toParquetOrAvroDir(parquetDir) + "/**/*.parquet";
        } else {
            parquetGlob = PathUtils.toParquetOrAvroDir(parquetDir) + "/*.parquet";
        }
        Schema schema = ParquetUtils.getAvroSchema(yarnConfiguration, parquetGlob);
        List<Pair<String, Class<?>>> fields = AvroUtils.parseSchema(schema);
        if (parquetDir.startsWith("/")) {
            String fs = yarnConfiguration.get("fs.defaultFS");
            parquetDir = fs + parquetDir;
        }
        log.info("Create table {} using parquet dir {}.", tableName, parquetDir);
        String createTableStmt;
        if (CollectionUtils.isNotEmpty(partitionKeys) && enableParquetPartition) {
            createTableStmt = PrestoUtils.getCreateParquetTableStmt(tableName, fields, partitionKeys, //
                    PathUtils.toParquetOrAvroDir(parquetDir));
        } else {
            createTableStmt = PrestoUtils.getCreateParquetTableStmt(tableName, fields, null, //
                    PathUtils.toParquetOrAvroDir(parquetDir));
        }
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        if (fields.size() > 1000) {
            log.warn("There are {} columns in table {}, table creation is going to be slow", fields.size(), tableName);
        }
        jdbcTemplate.execute(createTableStmt);
        if (CollectionUtils.isNotEmpty(partitionKeys) && enableParquetPartition) {
            String syncPartitionStmt = PrestoUtils.getSyncPartitionStmt("default", tableName);
            jdbcTemplate.execute(syncPartitionStmt);
        }
    }

    @Override
    public PrestoDataUnit saveDataUnit(HdfsDataUnit hdfsDataUnit) {
        String path = hdfsDataUnit.getPath();
        String tenantId = hdfsDataUnit.getTenant();
        String tableName = hdfsDataUnit.getName().toLowerCase();
        if (!tableName.startsWith(tenantId.toLowerCase())) {
            tableName = tenantId.toLowerCase() + "_" + tableName;
        }
        if (hdfsDataUnit.getDataFormat() == null) {
            hdfsDataUnit.setDataFormat(DataUnit.DataFormat.AVRO);
        }
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, path)) {
                List<Pair<String, Class<?>>> partitionKeys = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(hdfsDataUnit.getTypedPartitionKeys())) {
                    hdfsDataUnit.getTypedPartitionKeys().forEach(pair -> {
                        String key = pair.getLeft();
                        String clzName = pair.getRight();
                        Class<?> clz;
                        switch (clzName) {
                            case "String":
                                clz = String.class;
                                break;
                            case "Integer":
                                clz = Integer.class;
                                break;
                            case "Date":
                                clz = Date.class;
                                break;
                            default:
                                throw new UnsupportedOperationException("Unknown partition key type " + clzName);
                        }
                        partitionKeys.add(Pair.of(key, clz));
                    });
                } else if (CollectionUtils.isNotEmpty(hdfsDataUnit.getPartitionKeys())) {
                    hdfsDataUnit.getPartitionKeys().forEach(pk -> partitionKeys.add(Pair.of(pk, String.class)));
                }
                deleteTableIfExists(tableName);
                createTableIfNotExists(tableName, path, hdfsDataUnit.getDataFormat(), partitionKeys);
            } else {
                throw new IOException("Failed to create presto table, because cannot find hdfs path " + path);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create presto table by hdfs path: " + path, e);
        }
        PrestoDataUnit prestoDataUnit = new PrestoDataUnit();
        prestoDataUnit.setTenant(hdfsDataUnit.getTenant());
        prestoDataUnit.setName(hdfsDataUnit.getName());
        String clusterId = prestoConnectionService.getClusterId();
        prestoDataUnit.addPrestoTableName(clusterId, tableName);
        prestoDataUnit.setCount(getTableCount(tableName));
        if (CollectionUtils.isNotEmpty(hdfsDataUnit.getTypedPartitionKeys())) {
            prestoDataUnit.setTypedPartitionKeys(hdfsDataUnit.getTypedPartitionKeys());
        } else if (CollectionUtils.isNotEmpty(hdfsDataUnit.getPartitionKeys())) {
            prestoDataUnit.setPartitionKeys(hdfsDataUnit.getPartitionKeys());
        }
        return prestoDataUnit;
    }

    private long getTableCount(String tableName) {
        JdbcTemplate jdbcTemplate = getJdbcTemplate();
        Long cnt = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM " + tableName, Long.class);
        return cnt == null ? 0 : cnt;
    }

    private String getAvscPath(String tableName) {
        return "/presto-schema/avsc/" + tableName + ".avsc";
    }

    private Schema getDummySchema(String tableName) {
        List<Pair<String, Class<?>>> fields = Collections.singletonList( //
                Pair.of("Field1", String.class)
        );
        return AvroUtils.constructSchema(tableName, fields);
    }

}
