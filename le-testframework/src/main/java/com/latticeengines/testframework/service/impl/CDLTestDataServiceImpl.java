package com.latticeengines.testframework.service.impl;

import static com.latticeengines.domain.exposed.metadata.TableRoleInCollection.ConsolidatedAccount;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
import com.latticeengines.testframework.exposed.service.TestArtifactService;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@Service("cdlTestDataService")
public class CDLTestDataServiceImpl implements CDLTestDataService {

    private static final Logger log = LoggerFactory.getLogger(CDLTestDataServiceImpl.class);

    private static final String S3_DIR = "le-testframework/cdl";
    private static final Date DATE = new Date();
    private static final String POD_QA = "/Pods/QA/";
    private static final String POD_DEFAULT = "/Pods/Default/";
    private static final String POD_PATTERN = "/Pods/%s/";
    private static final String PATH_PATTERN = "/Contracts/(.*)/Tenants/";

    private static final Map<BusinessEntity, String> srcTables = new HashMap<>();

    private final TestArtifactService testArtifactService;
    private final MetadataProxy metadataProxy;
    private final DataCollectionProxy dataCollectionProxy;
    private final RedshiftPartitionService redshiftPartitionService;
    private final RatingEngineProxy ratingEngineProxy;
    private final BatonService batonService;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    @Value("${camille.zk.pod.id}")
    private String podId;

    @Inject
    private S3Service s3Service;

    @Inject
    private Configuration yarnConfiguration;

    private ObjectMapper om = new ObjectMapper();

    @Inject
    public CDLTestDataServiceImpl(TestArtifactService testArtifactService, MetadataProxy metadataProxy,
                                  DataCollectionProxy dataCollectionProxy, RedshiftPartitionService redshiftPartitionService,
                                  RatingEngineProxy ratingEngineProxy, BatonService batonService) {
        this.testArtifactService = testArtifactService;
        this.metadataProxy = metadataProxy;
        this.dataCollectionProxy = dataCollectionProxy;
        this.redshiftPartitionService = redshiftPartitionService;
        this.ratingEngineProxy = ratingEngineProxy;
        this.batonService = batonService;
        srcTables.put(BusinessEntity.Account, "cdl_test_account_%d");
        srcTables.put(BusinessEntity.Contact, "cdl_test_contact_%d");
        srcTables.put(BusinessEntity.Product, "cdl_test_product_%d");
        srcTables.put(BusinessEntity.Transaction, "cdl_test_transaction_%d");
        srcTables.put(BusinessEntity.PeriodTransaction, "cdl_test_period_transaction_%d");
        srcTables.put(BusinessEntity.DepivotedPurchaseHistory, "cdl_test_purchase_history_%d");
        srcTables.put(BusinessEntity.CuratedAccount, "cdl_test_curated_account_%d");
    }

    @Override
    public void populateMetadata(String tenantId, int version) {
        final String shortTenantId = CustomerSpace.parse(tenantId).getTenantId();
        dataCollectionProxy.getDefaultDataCollection(shortTenantId);
        if (dataCollectionProxy.getTableName(shortTenantId, TableRoleInCollection.BucketedAccount) != null) {
            DataCollection.Version active = dataCollectionProxy.getActiveVersion(shortTenantId);
            log.info("DataCollection version " + active + " is already populated, switch to " + active.complement());
            dataCollectionProxy.switchVersion(shortTenantId, active.complement());
        }
        List<Runnable> tasks = new ArrayList<>();
        ConcurrentMap<String, Long> entityCounts = new ConcurrentHashMap<>();
        tasks.add(() -> populateStats(shortTenantId, String.valueOf(version)));
        for (BusinessEntity entity : BusinessEntity.values()) {
            tasks.add(() -> populateServingStore(shortTenantId, entity, String.valueOf(version), entityCounts));

        }
        // Product data is needed for activity metrics metadata decorator
        tasks.add(() -> populateBatchStore(shortTenantId, BusinessEntity.Product, String.valueOf(version)));
        tasks.add(() -> populateTableRole(shortTenantId, ConsolidatedAccount, String.valueOf(version)));
        ThreadPoolUtils.runInParallel(tasks);
        updateDataCollectionStatus(shortTenantId, entityCounts);
    }

    @Override
    public void populateData(String tenantId, int version) {
        final String shortTenantId = CustomerSpace.parse(tenantId).getTenantId();
        dataCollectionProxy.getDefaultDataCollection(shortTenantId);
        if (dataCollectionProxy.getTableName(shortTenantId, TableRoleInCollection.BucketedAccount) != null) {
            DataCollection.Version active = dataCollectionProxy.getActiveVersion(shortTenantId);
            log.info("DataCollection version " + active + " is already populated, switch to " + active.complement());
            dataCollectionProxy.switchVersion(shortTenantId, active.complement());
        }
        List<Runnable> tasks = new ArrayList<>();
        ConcurrentMap<String, Long> entityCounts = new ConcurrentHashMap<>();
        tasks.add(() -> populateStats(shortTenantId, String.valueOf(version)));
        for (BusinessEntity entity : BusinessEntity.values()) {
            tasks.add(() -> {
                try (PerformanceTimer timer = new PerformanceTimer("Clone redshift table for " + entity)) {
                    cloneRedshiftTables(shortTenantId, entity, version);
                }
            });
            tasks.add(() -> populateServingStore(shortTenantId, entity, String.valueOf(version), entityCounts));
            tasks.add(() -> populateBatchStore(shortTenantId, entity, String.valueOf(version)));
        }
        ThreadPoolUtils.runInParallel(tasks);
        updateDataCollectionStatus(shortTenantId, entityCounts);
    }

    private void updateDataCollectionStatus(String shortTenantId, Map<String, Long> entityCounts) {
        DataCollection.Version active = dataCollectionProxy.getActiveVersion(shortTenantId);
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(shortTenantId, active);
        status.setAccountCount(entityCounts.getOrDefault("Account", 0L));
        status.setContactCount(entityCounts.getOrDefault("Contact", 0L));
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(shortTenantId, status, active);
    }

    @Override
    public void mockRatingTableWithSingleEngine(String tenantId, String engineId, //
                                                List<BucketMetadata> coverage) {
        mockRatingTableWithSingleEngine(tenantId, engineId, coverage, false);
    }

    @Override
    public void mockRatingTableWithSingleEngine(String tenantId, String engineId, //
                                                List<BucketMetadata> coverage, boolean uploadRatingTable) {
        if (CollectionUtils.isNotEmpty(coverage)) {
            mockRatingTable(tenantId, Collections.singletonList(engineId), ImmutableMap.of(engineId, coverage), uploadRatingTable);
        } else {
            mockRatingTable(tenantId, Collections.singletonList(engineId), null, uploadRatingTable);
        }
    }

    @Override
    public void mockRatingTable(String tenantId, List<String> engineIds, //
                                Map<String, List<BucketMetadata>> modelRatingBuckets, boolean uploadRatingTable) {
        tenantId = CustomerSpace.parse(tenantId).getTenantId();
        if (MapUtils.isEmpty(modelRatingBuckets)) {
            modelRatingBuckets = new HashMap<>();
        }
        for (String engineId : engineIds) {
            if (CollectionUtils.isEmpty(modelRatingBuckets.get(engineId))) {
                List<BucketMetadata> coverage = generateRandomBucketMetadata();
                modelRatingBuckets.put(engineId, coverage);
            }
        }
        String accountTblName = dataCollectionProxy.getTableName(tenantId, TableRoleInCollection.BucketedAccount);
        if (StringUtils.isBlank(accountTblName)) {
            throw new IllegalStateException("Cannot find BucketedAccount table for tenant " + tenantId);
        }
        String ratingTableName = NamingUtils.timestamp(tenantId + "_Rating");
        List<Pair<String, Class<?>>> columns = createRatingTable(tenantId, ratingTableName, engineIds);
        int maxCount;
        String msg = String.format("Mocking the rating table %s for engineIds %s using coverage %s", ratingTableName,
                engineIds, JsonUtils.serialize(modelRatingBuckets));
        JdbcTemplate redshiftJdbcTemplate = redshiftPartitionService.getBatchUserJdbcTemplate(null);
        List<List<Object>> data;
        try (PerformanceTimer timer = new PerformanceTimer(msg)) {
            maxCount = modelRatingBuckets.values().stream().map(m -> m.stream() //
                    .map(BucketMetadata::getNumLeads).reduce(0, (a, b) -> a + b)).max(Integer::compare).orElse(null);
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
                List<BucketMetadata> coverage = modelRatingBuckets.get(engineId);
                List<String> ratings = generateShuffledRatings(coverage);
                flux = flux.zipWith(Flux.fromIterable(ratings)).map(t -> {
                    t.getT1().add(t.getT2());
                    return t.getT1();
                });

                List<Double> scores = generateScores(tenantId, engineId, ratings);
                if (!CollectionUtils.isEmpty(scores)) {
                    flux = flux.zipWith(Flux.fromIterable(scores)).map(t -> {
                        t.getT1().add(t.getT2());
                        return t.getT1();
                    });
                }

                List<Double> evs = generateEV(tenantId, engineId, ratings);
                if (!CollectionUtils.isEmpty(evs)) {
                    flux = flux.zipWith(Flux.fromIterable(evs)).map(t -> {
                        t.getT1().add(t.getT2());
                        return t.getT1();
                    });
                }

            }
            data = flux.collectList().block();
            retry = getRedshiftRetryTemplate();
            RedshiftService redshiftService = redshiftPartitionService.getBatchUserService(null);
            retry.execute(context -> {
                log.info(String.format("(Attempt=%d) insert %d rows into rating table %s", context.getRetryCount() + 1,
                        CollectionUtils.size(data), ratingTableName));
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
            StatsCube statsCube = toStatsCube(modelRatingBuckets);
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
        final String finalTenantId = tenantId;
        Flux.fromIterable(engineIds).parallel().runOn(Schedulers.parallel()) //
                .map(engineId -> ratingEngineProxy.updateRatingEngineCounts(finalTenantId, engineId, null)) //
                .sequential().collectList().block();
        if (uploadRatingTable) {
            uploadRatingTable(finalTenantId, ratingTableName, schema, columns, data);
        }
    }

    private void deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                deleteDir(new File(dir, children[i]));
            }
        }
        dir.delete();
    }

    private void makeDir(String dirName) {
        File dir = new File(dirName);
        deleteDir(dir);
        dir.mkdir();
    }

    private void uploadRatingTable(String tenantId, String ratingTableName,
                                   Schema schema, List<Pair<String, Class<?>>> columns, List<List<Object>> data) {
        String dirName = tenantId;
        String fileName = "part-00000-" + UUID.randomUUID().toString() + ".avro";
        DatumWriter<GenericRecord> userDatumWriter = new GenericDatumWriter<>();
        if (CollectionUtils.isNotEmpty(data)) {
            makeDir(dirName);
            try {
                try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
                    dataFileWriter.create(schema, new File(dirName, fileName));
                    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
                    for (List<Object> objects : data) {
                        int index = 0;
                        for (Object object : objects) {
                            builder.set(columns.get(index).getLeft(), object.toString());
                            index++;
                        }
                        GenericRecord genericRecord = builder.build();
                        dataFileWriter.append(genericRecord);
                    }
                }
                CustomerSpace cs = CustomerSpace.parse(tenantId);
                String targetPath = PathBuilder.buildDataTablePath(podId, cs).toString() + "/" + ratingTableName;
                HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, dirName, targetPath);
            } catch (IOException exception) {
                log.info("Fail to upload rating table {}.", exception.getMessage());
            } finally {
                File dir = new File(dirName);
                deleteDir(dir);
            }
        }
    }

    private List<Double> generateEV(String tenantId, String engineId, List<String> ratings) {
        RatingEngine re = ratingEngineProxy.getRatingEngine(tenantId, engineId);
        if (re.getType() == RatingEngineType.CROSS_SELL && re.getPublishedIteration() != null
                && ((AIModel) re.getPublishedIteration()).getPredictionType() == PredictionType.EXPECTED_VALUE) {
            return Flux.fromIterable(ratings).map(bkt -> {
                switch (BucketName.fromValue(bkt)) {
                    case A:
                        return 95.0D * 1000;
                    case B:
                        return 70.0D * 1000;
                    case C:
                        return 40.0D * 1000;
                    case D:
                        return 20.0D * 1000;
                    case E:
                        return 10.0D * 1000;
                    case F:
                        return 5.0D * 1000;
                    default:
                        return 0.0D * 1000;
                }
            }).collectList().block();
        }
        return null;
    }

    private List<Double> generateScores(String tenantId, String engineId, List<String> ratings) {
        RatingEngine re = ratingEngineProxy.getRatingEngine(tenantId, engineId);
        if (re.getType() == RatingEngineType.RULE_BASED) {
            return null;
        }

        return Flux.fromIterable(ratings).map(bkt -> {
            switch (BucketName.fromValue(bkt)) {
                case A:
                    return 95.0D;
                case B:
                    return 70.0D;
                case C:
                    return 40.0D;
                case D:
                    return 20.0D;
                case E:
                    return 10.0D;
                case F:
                    return 5.0D;
                default:
                    return 0.0D;
            }
        }).collectList().block();
    }

    private StatsCube toStatsCube(Map<String, List<BucketMetadata>> coverages) {
        Integer maxCount = coverages.values().stream().flatMap(m -> m.stream().map(BucketMetadata::getNumLeads))
                .max(Integer::compare) //
                .orElse(null);
        AttributeStats accountIdStats = new AttributeStats();
        accountIdStats.setNonNullCount(maxCount.longValue());
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

    private AttributeStats toAttrStats(List<BucketMetadata> coverage) {
        AttributeStats attributeStats = new AttributeStats();
        Buckets buckets = new Buckets();
        buckets.setType(BucketType.Enum);

        long totalCount = 0L;
        List<Bucket> bucketList = new ArrayList<>();
        int bktId = 1;
        for (BucketMetadata entry : coverage) {
            String rating = entry.getBucketName();
            Integer count = entry.getNumLeads();
            Bucket bucket = new Bucket();
            bucket.setId((long) bktId++);
            bucket.setCount(count.longValue());
            bucket.setLabel(rating);
            bucketList.add(bucket);
            totalCount += count;
        }
        buckets.setBucketList(bucketList);
        attributeStats.setBuckets(buckets);

        attributeStats.setNonNullCount(totalCount);
        return attributeStats;
    }

    private List<Pair<String, Class<?>>> createRatingTable(String tenantId, String ratingTableName,
                                                           List<String> engineIds) {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        for (String engineId : engineIds) {
            schema.add(Pair.of(engineId, String.class));
            RatingEngine re = ratingEngineProxy.getRatingEngine(tenantId, engineId);
            if (re.getType() != RatingEngineType.RULE_BASED) {
                schema.add(Pair.of(engineId + PlaymakerConstants.RatingScoreColumnSuffix, String.class));
            }
            if (re.getType() == RatingEngineType.CROSS_SELL && re.getPublishedIteration() != null
                    && ((AIModel) re.getPublishedIteration()).getPredictionType() == PredictionType.EXPECTED_VALUE) {
                schema.add(Pair.of(engineId + PlaymakerConstants.RatingEVColumnSuffix, String.class));
            }
        }
        RedshiftService redshiftService = redshiftPartitionService.getBatchUserService(null);
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

    private List<String> generateShuffledRatings(List<BucketMetadata> bucketMetadata) {
        List<String> ratings = Flux.fromIterable(bucketMetadata).concatMap(bkt -> {
            String rating = bkt.getBucketName();
            int repeat = bkt.getNumLeads();
            return Flux.range(0, repeat).map(k -> rating);
        }).collectList().block();
        if (CollectionUtils.isNotEmpty(ratings)) {
            Collections.shuffle(ratings);
        }
        return ratings;
    }

    private List<BucketMetadata> generateRandomBucketMetadata() {
        Random random = new Random(System.currentTimeMillis());
        int countA = random.nextInt(200);
        int countB = random.nextInt(400 - countA);
        int countC = random.nextInt(700 - countA - countB);
        int countD = 1000 - countA - countB - countC;

        return Arrays.asList( //
                new BucketMetadata(BucketName.A, countA), //
                new BucketMetadata(BucketName.B, countB), //
                new BucketMetadata(BucketName.C, countC), //
                new BucketMetadata(BucketName.D, countD)//
        );
    }

    private void populateStats(String tenantId, String version) {
        String customerSpace = CustomerSpace.parse(tenantId).toString();
        StatisticsContainer container;
        try {
            InputStream is = testArtifactService.readTestArtifactAsStream(S3_DIR, version, "stats_container.json.gz");
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

    private void cloneRedshiftTables(String tenantId, BusinessEntity entity, int version) {
        if (srcTables.containsKey(entity)) {
            String srcTable = String.format(srcTables.get(entity), version);
            RedshiftService redshiftService = redshiftPartitionService.getBatchUserService(null);
            if (redshiftService.hasTable(srcTable)) {
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
    }

    private void populateServingStore(String tenantId, BusinessEntity entity, String s3Version, //
                                      ConcurrentMap<String, Long> entityCounts) {
        if (entity.getServingStore() != null) {
            Long count = populateTableRole(tenantId, entity.getServingStore(), s3Version);
            if (count != null) {
                entityCounts.put(entity.name(), count);
            }
        }
    }

    private Long populateTableRole(String tenantId, TableRoleInCollection role, String s3Version) {
        String customerSpace = CustomerSpace.parse(tenantId).toString();
        boolean entityMatchEnabled = batonService.isEntityMatchEnabled(CustomerSpace.parse(tenantId));
        Table table = readTableFromS3(role, s3Version, entityMatchEnabled);
        if (table != null) {
            String tableName = NamingUtils.timestamp(tenantId + "_" + role, DATE);
            table.setName(tableName);
            table.setDisplayName(role.name());
            metadataProxy.createTable(customerSpace, tableName, table);
            log.info("Metadata Table: {}, created with attributes: {}", tableName, table.getAttributes().size());
            DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace);
            dataCollectionProxy.upsertTable(customerSpace, tableName, role, activeVersion);
            if (CollectionUtils.isNotEmpty(table.getExtracts())) {
                try {
                    return table.getExtracts().get(0).getProcessedRecords();
                } catch (Exception e) {
                    log.warn("Failed to get " + role + " count.", e);
                }
            }
        }
        return null;
    }

    private Table readTableFromS3(TableRoleInCollection role, String version, boolean entityMatchEnabled) {
        try {
            String tableName = role.name() + ".json.gz";
            if (entityMatchEnabled
                    && testArtifactService.testArtifactExists(S3_DIR, version, role.name() + "_EM.json.gz")) {
                tableName = role.name() + "_EM.json.gz";
            }
            if (testArtifactService.testArtifactExists(S3_DIR, version, tableName)) {
                InputStream is = testArtifactService.readTestArtifactAsStream(S3_DIR, version, tableName);
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
            } else {
                return null;
            }
        } catch (Exception e) {
            log.error(String.format("Role: %s, version:%s", role.name(), version));
            return null;
        }
    }

    private String servingStoreName(String tenantId, BusinessEntity entity) {
        return NamingUtils.timestamp(tenantId + "_" + entity.getServingStore().name(), DATE);
    }

    private void populateBatchStore(String tenantId, BusinessEntity entity, String version) {
        if (entity.getBatchStore() != null && testArtifactService.testArtifactExists(S3_DIR, version,
                entity.getBatchStore().name() + ".json.gz")) {
            String customerSpace = CustomerSpace.parse(tenantId).toString();
            // populate metadata
            populateTableRole(tenantId, entity.getBatchStore(), version);
            // populate S3 data
            if (testArtifactService.testArtifactFolderExists(S3_DIR, version, entity.getBatchStore().name())) {
                HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
                String tableName = dataCollectionProxy.getTableName(customerSpace, entity.getBatchStore());
                String s3Prefix = pathBuilder.getS3AtlasTablePrefix(tenantId, tableName);
                log.info("Copy {}'s batch store {} to {}", entity.name(), entity.getBatchStore().name(), s3Prefix);
                testArtifactService.copyTestArtifactFolder(S3_DIR, version, entity.getBatchStore().name(), s3Bucket,
                        s3Prefix);

            }
        }
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

    @Override
    public String createLaunchTable(String tenantId, String s3AvroDir, String version, String tableName) throws IOException {
        File jsonFile = testArtifactService.downloadTestArtifact(s3AvroDir, version, tableName + ".json");
        String shortenTenantId = CustomerSpace.parse(tenantId).getTenantId();
        Table table = toTable(jsonFile, shortenTenantId);
        String tablePath = table.getExtracts().get(0).getPath();
        metadataProxy.createTable(tenantId, table.getName(), table);
        log.info("Created " + table + " at " + tablePath);
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
        String tgtDir = pathBuilder.getS3AtlasTablePrefix(shortenTenantId, table.getName());
        testArtifactService.copyTestArtifactFolder(s3AvroDir, version, tableName, s3Bucket, tgtDir);
        return table.getName();
    }

    private Table toTable(File jsonFile, String tenantId) throws IOException {
        JsonNode jsonNode = om.readTree(jsonFile);
        String hdfsPath = jsonNode.get("extracts").get(0).get("path").asText();
        if (hdfsPath.endsWith(".avro") || hdfsPath.endsWith("/")) {
            hdfsPath = hdfsPath.substring(0, hdfsPath.lastIndexOf("/"));
        }
        log.info("Parse extract path {}.", hdfsPath);
        Pattern pattern = Pattern.compile(PATH_PATTERN);
        Matcher matcher = pattern.matcher(hdfsPath);
        String str = JsonUtils.serialize(jsonNode);
        str = str.replaceAll(POD_QA, String.format(POD_PATTERN, podId));
        str = str.replaceAll(POD_DEFAULT, String.format(POD_PATTERN, podId));
        String tenantName = null;
        if (matcher.find()) {
            tenantName = matcher.group(1);
            log.info("Found tenant name {} in json.", tenantName);
        }
        if (StringUtils.isNotEmpty(tenantName)) {
            String testTenant = CustomerSpace.parse(tenantId).getTenantId();
            str = str.replaceAll(tenantName, testTenant);
        }
        return JsonUtils.deserialize(str, Table.class);
    }

    @Override
    public void uploadAvroToS3(File file, String tenantId, String tableName) {
        HdfsToS3PathBuilder hdfsToS3PathBuilder = new HdfsToS3PathBuilder(useEmr);
        String prefix = hdfsToS3PathBuilder.getS3AtlasTablePrefix(tenantId, tableName);
        s3Service.uploadLocalFile(s3Bucket, prefix, file, true);
    }
}
