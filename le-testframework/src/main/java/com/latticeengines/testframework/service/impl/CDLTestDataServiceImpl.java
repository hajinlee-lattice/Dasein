package com.latticeengines.testframework.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
import com.latticeengines.testframework.exposed.service.TestArtifactService;

import reactor.core.publisher.Flux;

@Service("cdlTestDataService")
public class CDLTestDataServiceImpl implements CDLTestDataService {

    private static final Logger log = LoggerFactory.getLogger(CDLTestDataServiceImpl.class);

    private static final String S3_DIR = "le-testframework/cdl";
    private static final String S3_VERSION = "3";
    private static final Date DATE = new Date();

    private static final ImmutableMap<BusinessEntity, String> srcTables = ImmutableMap.of( //
            BusinessEntity.Account, "cdl_test_account_2", //
            BusinessEntity.Contact, "cdl_test_contact_2", //
            BusinessEntity.Product, "cdl_test_product_2", //
            BusinessEntity.Transaction, "cdl_test_transaction_2", //
            BusinessEntity.PeriodTransaction, "cdl_test_period_transaction_2" //
    );

    private final TestArtifactService testArtifactService;
    private final MetadataProxy metadataProxy;
    private final DataCollectionProxy dataCollectionProxy;
    private final RedshiftService redshiftService;

    @Resource(name = "redshiftJdbcTemplate")
    private JdbcTemplate redshiftJdbcTemplate;

    @Inject
    public CDLTestDataServiceImpl(TestArtifactService testArtifactService, MetadataProxy metadataProxy,
            DataCollectionProxy dataCollectionProxy, RedshiftService redshiftService) {
        this.testArtifactService = testArtifactService;
        this.metadataProxy = metadataProxy;
        this.dataCollectionProxy = dataCollectionProxy;
        this.redshiftService = redshiftService;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void populateData(String tenantId) {
        final String shortTenantId = CustomerSpace.parse(tenantId).getTenantId();
        dataCollectionProxy.getDefaultDataCollection(shortTenantId);
        if (dataCollectionProxy.getTableName(shortTenantId, TableRoleInCollection.BucketedAccount) != null) {
            DataCollection.Version active = dataCollectionProxy.getActiveVersion(shortTenantId);
            log.info("DataCollection version " + active + " is already populated, switch to " + active.complement());
            dataCollectionProxy.switchVersion(shortTenantId, active.complement());
        }
        ExecutorService executors = ThreadPoolUtils.getFixedSizeThreadPool("cdl-test-data", 4);
        Set<Future> futures = new HashSet<>();
        futures.add(executors.submit(() -> {
            populateStats(shortTenantId);
            return true;
        }));
        for (BusinessEntity entity : BusinessEntity.values()) {
            futures.add(executors.submit(() -> {
                try (PerformanceTimer timer = new PerformanceTimer("Clone redshift table for " + entity)) {
                    cloneRedshiftTables(shortTenantId, entity);
                }
                return true;
            }));
            futures.add(executors.submit(() -> {
                populateServingStore(shortTenantId, entity);
                return true;
            }));
        }
        while (!futures.isEmpty()) {
            Set<Future> toBeDeleted = new HashSet<>();
            futures.forEach(future -> {
                try {
                    future.get(10, TimeUnit.SECONDS);
                    toBeDeleted.add(future);
                } catch (TimeoutException e) {
                    // ignore
                } catch (Exception e) {
                    throw new RuntimeException("One of the future is failed", e);
                }
            });

            futures.removeAll(toBeDeleted);
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void mockRatingTableWithSingleEngine(String tenantId, String engineId, //
            Map<RatingBucketName, Long> coverage) {
        if (MapUtils.isNotEmpty(coverage)) {
            mockRatingTable(tenantId, Collections.singletonList(engineId), ImmutableMap.of(engineId, coverage));
        } else {
            mockRatingTable(tenantId, Collections.singletonList(engineId), null);
        }
    }

    public void mockRatingTable(String tenantId, List<String> engineIds, //
            Map<String, Map<RatingBucketName, Long>> coverages) {
        tenantId = CustomerSpace.parse(tenantId).getTenantId();
        if (MapUtils.isEmpty(coverages)) {
            coverages = new HashMap<>();
        }
        for (String engineId : engineIds) {
            if (MapUtils.isEmpty(coverages.get(engineId))) {
                Map<RatingBucketName, Long> coverage = generateRandomCoverage();
                coverages.put(engineId, coverage);
            }
        }
        String accountTblName = dataCollectionProxy.getTableName(tenantId, TableRoleInCollection.BucketedAccount);
        String ratingTableName = NamingUtils.timestamp(tenantId + "_Rating");
        List<Pair<String, Class<?>>> columns = createRatingTable(ratingTableName, engineIds);
        Long maxCount;
        String msg = String.format("Mocking the rating table %s for engineIds %s using coverage %s", ratingTableName,
                engineIds, JsonUtils.serialize(coverages));
        try (PerformanceTimer timer = new PerformanceTimer(msg)) {
            maxCount = coverages.values().stream().map(m -> m.values().stream() //
                    .reduce(0L, (a, b) -> a + b)).max(Long::compare).orElse(null);
            log.info("Maximum count cross all engines is " + maxCount);
            String selectAccountIds = "SELECT AccountId FROM " + accountTblName + " LIMIT " + maxCount + ";";
            RetryTemplate retry = getRedshiftRetryTemplate();
            List<String> accountIds = retry.execute(context -> {
                log.info(String.format("(Attempt=%d) query account ids from rating table %s",
                        context.getRetryCount() + 1, ratingTableName));
                return redshiftJdbcTemplate.queryForList(selectAccountIds, String.class);
            });
            Flux<List<Object>> flux = Flux.fromIterable(accountIds).map(aid -> {
                List<Object> row = new ArrayList<>();
                row.add(aid);
                return row;
            });
            for (String engineId : engineIds) {
                Map<RatingBucketName, Long> coverage = coverages.get(engineId);
                List<String> ratings = generateShuffledRatings(coverage);
                flux = flux.zipWith(Flux.fromIterable(ratings)).map(t -> {
                    t.getT1().add(t.getT2());
                    return t.getT1();
                });
            }
            List<List<Object>> data = flux.collectList().block();
            retry = getRedshiftRetryTemplate();
            retry.execute(context -> {
                log.info(String.format("(Attempt=%d) insert %d rows into rating table %s", context.getRetryCount() + 1,
                        data.size(), ratingTableName));
                redshiftJdbcTemplate.execute("DELETE FROM " + ratingTableName + ";");
                redshiftService.insertValuesIntoTable(ratingTableName, columns, data);
                return null;
            });
        }

        msg = String.format("Inserting rating stats for %d engines.", engineIds.size());
        try (PerformanceTimer timer = new PerformanceTimer(msg)) {
            StatisticsContainer container = dataCollectionProxy.getStats(tenantId);
            Map<String, StatsCube> cubes = container.getStatsCubes();
            if (MapUtils.isEmpty(cubes)) {
                cubes = new HashMap<>();
            }
            StatsCube statsCube = toStatsCube(coverages);
            cubes.put(BusinessEntity.Rating.name(), statsCube);
            container.setStatsCubes(cubes);
            container.setName(NamingUtils.timestamp("Stats"));
            dataCollectionProxy.upsertStats(tenantId, container);
        }

        Schema schema = AvroUtils.constructSchema(ratingTableName, columns);
        Table table = MetadataConverter.getTable(schema, null, InterfaceName.AccountId.name(), null, false);
        metadataProxy.createTable(tenantId, ratingTableName, table);
        DataCollection.Version active = dataCollectionProxy.getActiveVersion(tenantId);
        dataCollectionProxy.upsertTable(tenantId, ratingTableName, TableRoleInCollection.PivotedRating, active);
    }

    private StatsCube toStatsCube(Map<String, Map<RatingBucketName, Long>> coverages) {
        Long maxCount = coverages.values().stream().flatMap(m -> m.values().stream()).max(Long::compare) //
                .orElse(null);
        AttributeStats accountIdStats = new AttributeStats();
        accountIdStats.setNonNullCount(maxCount);
        Map<String, AttributeStats> statistics = new HashMap<>();
        statistics.put(InterfaceName.AccountId.name(), accountIdStats);
        coverages.forEach((engineId, coverage) -> {
            AttributeStats attrStats = toAttrStats(coverage);
            statistics.put(engineId, attrStats);
        });
        StatsCube statsCube = new StatsCube();
        statsCube.setStatistics(statistics);
        return statsCube;
    }

    private AttributeStats toAttrStats(Map<RatingBucketName, Long> coverage) {
        AttributeStats attributeStats = new AttributeStats();
        Buckets buckets = new Buckets();
        buckets.setType(BucketType.Enum);

        long totalCount = 0L;
        List<Bucket> bucketList = new ArrayList<>();
        int bktId = 1;
        for (Map.Entry<RatingBucketName, Long> entry : coverage.entrySet()) {
            String rating = entry.getKey().name();
            Long count = entry.getValue();
            Bucket bucket = new Bucket();
            bucket.setId((long) bktId++);
            bucket.setCount(count);
            bucket.setLabel(rating);
            bucketList.add(bucket);
            totalCount += count;
        }
        buckets.setBucketList(bucketList);
        attributeStats.setBuckets(buckets);

        attributeStats.setNonNullCount(totalCount);
        return attributeStats;
    }

    private List<Pair<String, Class<?>>> createRatingTable(String ratingTableName, List<String> engineIds) {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        for (String engineId : engineIds) {
            schema.add(Pair.of(engineId, String.class));
        }
        RetryTemplate retry = getRedshiftRetryTemplate();
        retry.execute((RetryCallback<Void, RuntimeException>) context -> {
            log.info(
                    String.format("(Attempt=%d) create rating table %s", context.getRetryCount() + 1, ratingTableName));
            if (!redshiftService.hasTable(ratingTableName)) {
                RedshiftTableConfiguration configuration = new RedshiftTableConfiguration();
                configuration.setTableName(ratingTableName);
                configuration.setDistKey(InterfaceName.AccountId.name());
                configuration.setDistStyle(RedshiftTableConfiguration.DistStyle.Key);
                redshiftService.createTable(configuration, AvroUtils.constructSchema(ratingTableName, schema));
            }
            return null;
        });
        return schema;
    }

    private List<String> generateShuffledRatings(Map<RatingBucketName, Long> coverage) {
        List<String> ratings = Flux.fromIterable(coverage.entrySet()).concatMap(e -> {
            String rating = e.getKey().name();
            int repeat = e.getValue().intValue();
            return Flux.range(0, repeat).map(k -> rating);
        }).collectList().block();
        if (CollectionUtils.isNotEmpty(ratings)) {
            Collections.shuffle(ratings);
        }
        return ratings;
    }

    private Map<RatingBucketName, Long> generateRandomCoverage() {
        Random random = new Random(System.currentTimeMillis());
        int countA = random.nextInt(700);
        int countB = random.nextInt(800 - countA);
        int countC = random.nextInt(900 - countA - countB);
        int countD = 1000 - countA - countB - countC;
        return ImmutableMap.of( //
                RatingBucketName.A, (long) countA, //
                RatingBucketName.B, (long) countB, //
                RatingBucketName.C, (long) countC, //
                RatingBucketName.D, (long) countD //
        );
    }

    private void populateStats(String tenantId) {
        String customerSpace = CustomerSpace.parse(tenantId).toString();
        StatisticsContainer container;
        try {
            InputStream is = testArtifactService.readTestArtifactAsStream(S3_DIR, S3_VERSION,
                    "stats_container.json.gz");
            GZIPInputStream gis = new GZIPInputStream(is);
            String content = IOUtils.toString(gis, Charset.forName("UTF-8"));
            ObjectMapper om = new ObjectMapper();
            container = om.readValue(content, StatisticsContainer.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to download from S3 and parse stats container", e);
        }
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace);
        container.setVersion(activeVersion);
        container.setName(NamingUtils.timestamp("Stats"));
        dataCollectionProxy.upsertStats(customerSpace, container);
    }

    private void cloneRedshiftTables(String tenantId, BusinessEntity entity) {
        if (srcTables.containsKey(entity)) {
            String srcTable = srcTables.get(entity);
            String tgtTable = servingStoreName(tenantId, entity);
            RetryTemplate retry = getRedshiftRetryTemplate();
            retry.execute((RetryCallback<Void, RuntimeException>) context -> {
                log.info(String.format("(Attempt=%d) copying %s to %s", context.getRetryCount() + 1, srcTable,
                        tgtTable));
                if (!redshiftService.hasTable(tgtTable)) {
                    redshiftService.cloneTable(srcTable, tgtTable);
                } else {
                    log.info("Seems table " + tgtTable + " already exists.");
                }
                return null;
            });
        }
    }

    private void populateServingStore(String tenantId, BusinessEntity entity) {
        if (Arrays.asList( //
                BusinessEntity.Account, //
                BusinessEntity.Contact, //
                BusinessEntity.Product, //
                BusinessEntity.Transaction, //
                BusinessEntity.PeriodTransaction).contains(entity)) {
            String customerSpace = CustomerSpace.parse(tenantId).toString();
            Table table = readTableFromS3(entity);
            String tableName = servingStoreName(tenantId, entity);
            table.setName(tableName);
            table.setDisplayName(entity.getServingStore().name());
            metadataProxy.createTable(customerSpace, tableName, table);
            DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace);
            dataCollectionProxy.upsertTable(customerSpace, tableName, entity.getServingStore(), activeVersion);
        }
    }

    private Table readTableFromS3(BusinessEntity entity) {
        TableRoleInCollection role = entity.getServingStore();
        InputStream is = testArtifactService.readTestArtifactAsStream(S3_DIR, S3_VERSION, role.name() + ".json.gz");
        Table table;
        try {
            GZIPInputStream gis = new GZIPInputStream(is);
            String content = IOUtils.toString(gis, Charset.forName("UTF-8"));
            ObjectMapper om = new ObjectMapper();
            table = om.readValue(content, Table.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse the table json", e);
        }
        table.setTableType(TableType.DATATABLE);
        return table;
    }

    private String servingStoreName(String tenantId, BusinessEntity entity) {
        return NamingUtils.timestamp(tenantId + "_" + entity.name(), DATE);
    }

    private RetryTemplate getRedshiftRetryTemplate() {
        RetryTemplate retry = new RetryTemplate();
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(3);
        retry.setRetryPolicy(retryPolicy);
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(2000);
        backOffPolicy.setMultiplier(2.0);
        retry.setBackOffPolicy(backOffPolicy);
        retry.setThrowLastExceptionOnExhausted(true);
        return retry;
    }

}
