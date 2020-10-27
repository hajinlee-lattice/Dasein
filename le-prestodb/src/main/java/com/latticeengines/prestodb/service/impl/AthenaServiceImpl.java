package com.latticeengines.prestodb.service.impl;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.athena.model.ColumnInfo;
import com.amazonaws.services.athena.model.Datum;
import com.amazonaws.services.athena.model.GetQueryExecutionRequest;
import com.amazonaws.services.athena.model.GetQueryExecutionResult;
import com.amazonaws.services.athena.model.GetQueryResultsRequest;
import com.amazonaws.services.athena.model.GetQueryResultsResult;
import com.amazonaws.services.athena.model.QueryExecutionContext;
import com.amazonaws.services.athena.model.QueryExecutionState;
import com.amazonaws.services.athena.model.Row;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.amazonaws.services.athena.model.StartQueryExecutionResult;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.ParquetUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.metadata.datastore.AthenaDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.prestodb.exposed.service.AthenaService;
import com.latticeengines.prestodb.util.AthenaUtils;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;


@Service
public class AthenaServiceImpl implements AthenaService  {

    private static final Logger log = LoggerFactory.getLogger(AthenaServiceImpl.class);

    @Value("${prestodb.athena.database}")
    private String database;

    @Value("${prestodb.athena.workgroup}")
    private String workgroup;

    @Value("${aws.region}")
    private String region;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Inject
    private S3Service s3Service;

    @Resource(name = "awsCredentials")
    private AWSCredentials awsCredentials;

    @Inject
    private Configuration yarnConfiguration;

    private AmazonAthena athena;

    @Override
    public boolean tableExists(String tableName) {
        String checkStmt = AthenaUtils.getCheckTableStmt(database, tableName);
        return Boolean.TRUE.equals(queryForObject(checkStmt, Boolean.class));
    }

    @Override
    public void deleteTableIfExists(String tableName) {
        log.info("Deleting table {} if not exists", tableName);
        String stmt = AthenaUtils.getDeleteTableStmt(tableName);
        execute(stmt);
    }

    @Override
    public List<String> getTablesStartsWith(String tableNamePrefix) {
        String stmt = AthenaUtils.getTableNamesStartsWith(database, tableNamePrefix);
        return queryForList(stmt, String.class);
    }

    @Override
    public void createTableIfNotExists(String tableName, String s3Bucket, String s3Prefix, DataUnit.DataFormat format, //
                                       List<Pair<String, Class<?>>> partitionKeys) {
        String s3Protocol = Boolean.TRUE.equals(useEmr) ? "s3a://" : "s3n://";
        if (!s3Prefix.startsWith("/")) {
            s3Prefix = "/" + s3Prefix;
        }
        String s3Dir = s3Protocol + s3Bucket + s3Prefix;
        if (DataUnit.DataFormat.AVRO.equals(format)) {
            createAvroTable(tableName, s3Dir, partitionKeys);
        } else if (DataUnit.DataFormat.PARQUET.equals(format)) {
            createParquetTable(tableName, s3Dir, partitionKeys);
        } else {
            throw new UnsupportedOperationException("Unknown data type " + format);
        }
    }

    @Override
    public AthenaDataUnit saveDataUnit(S3DataUnit s3DataUnit) {
        String bucket = s3DataUnit.getBucket();
        String prefix = s3DataUnit.getPrefix();
        String tenantId = s3DataUnit.getTenant();
        String tableName = s3DataUnit.getName().toLowerCase();
        if (!tableName.startsWith(tenantId.toLowerCase())) {
            tableName = tenantId.toLowerCase() + "_" + tableName;
        }
        if (s3DataUnit.getDataFormat() == null) {
            s3DataUnit.setDataFormat(DataUnit.DataFormat.AVRO);
        }
        try {
            if (s3Service.isNonEmptyDirectory(bucket, prefix)) {
                List<Pair<String, Class<?>>> partitionKeys = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(s3DataUnit.getTypedPartitionKeys())) {
                    s3DataUnit.getTypedPartitionKeys().forEach(pair -> {
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
                } else if (CollectionUtils.isNotEmpty(s3DataUnit.getPartitionKeys())) {
                    s3DataUnit.getPartitionKeys().forEach(pk -> partitionKeys.add(Pair.of(pk, String.class)));
                }
                deleteTableIfExists(tableName);
                createTableIfNotExists(tableName, bucket, prefix, s3DataUnit.getDataFormat(), partitionKeys);
            } else {
                throw new IOException("Failed to create athena table, because cannot find s3 prefix " //
                        + prefix + " in bucket " + bucket);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to create athena table by hdfs path: s3 prefix " //
                    + prefix + " in bucket " + bucket, e);
        }

        AthenaDataUnit athenaDataUnit = new AthenaDataUnit();
        athenaDataUnit.setTenant(s3DataUnit.getTenant());
        athenaDataUnit.setName(s3DataUnit.getName());
        athenaDataUnit.setAthenaTable(tableName);
        athenaDataUnit.setCount(getTableCount(tableName));
        if (CollectionUtils.isNotEmpty(s3DataUnit.getTypedPartitionKeys())) {
            athenaDataUnit.setTypedPartitionKeys(s3DataUnit.getTypedPartitionKeys());
        } else if (CollectionUtils.isNotEmpty(s3DataUnit.getPartitionKeys())) {
            athenaDataUnit.setPartitionKeys(s3DataUnit.getPartitionKeys());
        }
        return athenaDataUnit;
    }

    private long getTableCount(String tableName) {
        Long cnt = queryForObject(String.format("SELECT COUNT(1) FROM %s", tableName), Long.class);
        return cnt == null ? 0 : cnt;
    }

    private void createAvroTable(String tableName, String avroDir, List<Pair<String, Class<?>>> partitionKeys) {
        String avroGlob;
        if (CollectionUtils.isNotEmpty(partitionKeys)) {
            avroGlob = PathUtils.toParquetOrAvroDir(avroDir) + "/**/*.avro";
        } else {
            avroGlob = PathUtils.toParquetOrAvroDir(avroDir) + "/*.avro";
        }
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        List<Pair<String, Class<?>>> fields = AvroUtils.parseSchema(schema);
        schema = AvroUtils.constructSchema(schema.getName(), fields);
        List<String> createStmts = AthenaUtils.getCreateAvroTableStmt(tableName, schema, partitionKeys, //
                PathUtils.toParquetOrAvroDir(avroDir));
        for (String createStmt: createStmts) {
            execute(createStmt);
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
        schema = AvroUtils.constructSchema(schema.getName(), fields);
        List<String> createStmts = AthenaUtils.getCreateParquetTableStmt(tableName, schema, partitionKeys, //
                PathUtils.toParquetOrAvroDir(parquetDir));
        for (String createStmt: createStmts) {
            execute(createStmt);
        }
    }


    @Override
    public <T> List<T> queryForList(String sql, Class<T> clz) {
        Flux<Map<String, Object>> flux = queryFlux(sql);
        return flux.map(row -> {
            Object val = row.values().toArray()[0];
            return clz.cast(val);
        }).collectList().block(Duration.ofHours(1));
    }

    @Override
    public List<Map<String, Object>> query(String sql) {
        Flux<Map<String, Object>> flux = queryFlux(sql);
        return flux.collectList().block(Duration.ofHours(1));
    }

    @Override
    public Flux<Map<String, Object>> queryFlux(String sql) {
        String executionId = submitQuery(sql);
        log.info("sql={}", sql.replaceAll("\n", " "));
        waitForQueryToComplete(executionId);
        return Flux.generate(() -> {
            GetQueryResultsRequest queryRequest = new GetQueryResultsRequest()
                    .withQueryExecutionId(executionId);
            GetQueryResultsResult queryResults = getAthena().getQueryResults(queryRequest);
            List<ColumnInfo> columnInfoList = queryResults.getResultSet().getResultSetMetadata().getColumnInfo();
            Queue<Row> queue = new LinkedList<>(queryResults.getResultSet().getRows());
            log.info("Skipping header line = {}", queue.poll());
            String nextToken = queryResults.getNextToken();
            return Tuples.of(queue, columnInfoList, StringUtils.defaultIfBlank(nextToken, ""));
        }, (state, sink) -> {
            Queue<Row> queue = state.getT1();
            List<ColumnInfo> columnInfoList = state.getT2();
            String nextToken = state.getT3();
            while (queue.isEmpty() && StringUtils.isNotBlank(nextToken)) {
                GetQueryResultsRequest queryRequest = new GetQueryResultsRequest() //
                        .withQueryExecutionId(executionId).withNextToken(nextToken);
                GetQueryResultsResult queryResults = getAthena().getQueryResults(queryRequest);
                queue = new LinkedList<>(queryResults.getResultSet().getRows());
                columnInfoList = queryResults.getResultSet().getResultSetMetadata().getColumnInfo();
                nextToken = queryResults.getNextToken();
            }

            if (queue.isEmpty()) {
                sink.complete();
            } else {
                Row row = queue.poll();
                Map<String, Object> map = processRow(row, columnInfoList);
                sink.next(map);
            }

            return Tuples.of(queue, columnInfoList, StringUtils.defaultIfBlank(nextToken, ""));
        });
    }

    @Override
    public <T> T queryObject(String sql, Class<T> clz) {
        return queryForObject(sql, clz);
    }

    private void execute(String sql) {
        String executionId = submitQuery(sql);
        waitForQueryToComplete(executionId);
    }

    private <T> T queryForObject(String sql, Class<T> clz) {
        String executionId = submitQuery(sql);
        waitForQueryToComplete(executionId);
        return getResultObject(executionId, clz);
    }

    private String submitQuery(String sql) {
        QueryExecutionContext queryExecutionContext = new QueryExecutionContext().withDatabase(database);
        StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest() //
                .withQueryString(sql) //
                .withQueryExecutionContext(queryExecutionContext) //
                .withWorkGroup(workgroup);

        AmazonAthena athena = getAthena();
        StartQueryExecutionResult startQueryExecutionResult = athena.startQueryExecution(startQueryExecutionRequest);
        String executionId = startQueryExecutionResult.getQueryExecutionId();
        String compactSql = sql.replaceAll("\n", " ").replaceAll("\\s+", " ");
        log.info("Submitted Athena executionId={}, sql={}", executionId, compactSql);
        return executionId;
    }

    private void waitForQueryToComplete(String queryExecutionId) {
        try (PerformanceTimer timer = new PerformanceTimer( //
                "Athena query executionId=" + queryExecutionId)) {
            AmazonAthena athena = getAthena();
            GetQueryExecutionRequest getQueryExecutionRequest = new GetQueryExecutionRequest()
                    .withQueryExecutionId(queryExecutionId);

            GetQueryExecutionResult getQueryExecutionResult;
            boolean isQueryStillRunning = true;
            long lastLogging = System.currentTimeMillis();
            long waitInterval = 100L;
            while (isQueryStillRunning) {
                getQueryExecutionResult = athena.getQueryExecution(getQueryExecutionRequest);
                String queryState = getQueryExecutionResult.getQueryExecution().getStatus().getState();
                if (QueryExecutionState.FAILED.toString().equals(queryState)) {
                    throw new RuntimeException("Query Failed to run with Error Message: " + getQueryExecutionResult.getQueryExecution().getStatus().getStateChangeReason());
                } else if (QueryExecutionState.CANCELLED.toString().equals(queryState)) {
                    throw new RuntimeException("Query was cancelled.");
                } else if (QueryExecutionState.SUCCEEDED.toString().equals(queryState)) {
                    isQueryStillRunning = false;
                } else {
                    // Sleep an amount of time before retrying again.
                    SleepUtils.sleep(waitInterval);
                    if (waitInterval < TimeUnit.MINUTES.toMillis(1)) {
                        waitInterval *= 1.18;
                    }
                }
                long now = System.currentTimeMillis();
                if (now - lastLogging > 10000L) {
                    log.info("Current status of query {} is: {}", queryExecutionId, queryState);
                    lastLogging = now;
                }
            }
        }
    }

    private <T> T getResultObject(String queryExecutionId, Class<T> clz) {
        List<Map<String, Object>> resultList = processResultRows(queryExecutionId);
        if (CollectionUtils.size(resultList) != 1 || MapUtils.size(resultList.get(0)) != 1) {
            throw new RuntimeException("The query did not return single result, cannot be parsed to an object: " //
                    + resultList);
        }
        Object val = resultList.get(0).values().toArray()[0];
        return clz.cast(val);
    }

    private List<Map<String, Object>> processResultRows(String queryExecutionId)    {
        GetQueryResultsRequest getQueryResultsRequest = new GetQueryResultsRequest()
                .withQueryExecutionId(queryExecutionId);
        GetQueryResultsResult getQueryResultsResult = getAthena().getQueryResults(getQueryResultsRequest);
        List<ColumnInfo> columnInfoList = getQueryResultsResult.getResultSet().getResultSetMetadata().getColumnInfo();
        List<Map<String, Object>> maps = new ArrayList<>();
        boolean isHeaders = true;
        while (true) {
            List<Row> results = getQueryResultsResult.getResultSet().getRows();
            for (Row row : results) {
                if (isHeaders) {
                    isHeaders = false;
                } else {
                    maps.add(processRow(row, columnInfoList));
                }
            }
            // If nextToken is null, there are no more pages to read. Break out of the loop.
            if (getQueryResultsResult.getNextToken() == null) {
                break;
            }
            getQueryResultsResult = getAthena().getQueryResults(
                    getQueryResultsRequest.withNextToken(getQueryResultsResult.getNextToken()));
        }
        return maps;
    }

    private static Map<String, Object> processRow(Row row, List<ColumnInfo> columnInfoList) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < columnInfoList.size(); ++i) {
            ColumnInfo columnInfo = columnInfoList.get(i);
            Object val = null;
            Datum datum = row.getData().get(i);
            if (datum != null && datum.getVarCharValue() != null) {
                switch (columnInfo.getType()) {
                    case "varchar":
                        val = datum.getVarCharValue();
                        break;
                    case "tinyint":
                    case "smallint":
                    case "integer":
                        val = Integer.valueOf(datum.getVarCharValue());
                        break;
                    case "bigint":
                        val = Long.valueOf(datum.getVarCharValue());
                        break;
                    case "double":
                        val = Double.valueOf(datum.getVarCharValue());
                        break;
                    case "boolean":
                        val = Boolean.valueOf(datum.getVarCharValue());
                        break;
                    case "date":
                    case "timestamp":
                    default:
                        throw new UnsupportedOperationException("Unexpected Type is not expected" + columnInfoList.get(i).getType());
                }
            }
            map.put(columnInfo.getName(), val);
        }
        return map;
    }

    private AmazonAthena getAthena() {
        if (athena == null) {
            athena = AmazonAthenaClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials)) //
                    .withRegion(Regions.fromName(region)) //
                    .build();
        }
        return athena;
    }

}
