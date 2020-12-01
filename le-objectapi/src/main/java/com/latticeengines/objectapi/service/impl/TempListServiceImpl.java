package com.latticeengines.objectapi.service.impl;

import static com.latticeengines.query.factory.AthenaQueryProvider.ATHENA_USER;
import static com.latticeengines.query.factory.PrestoQueryProvider.PRESTO_USER;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.camille.exposed.locks.LockManager;
import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.CollectionLookup;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.TempListUtils;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.util.RestrictionUtils;
import com.latticeengines.objectapi.service.TempListService;
import com.latticeengines.prestodb.exposed.service.AthenaService;
import com.latticeengines.prestodb.exposed.service.PrestoConnectionService;
import com.latticeengines.prestodb.exposed.service.PrestoDbService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

@Service("tempListService")
public class TempListServiceImpl implements TempListService {

    private static final Logger log = LoggerFactory.getLogger(TempListServiceImpl.class);

    private static final String CACHE_PREFIX = "RedShiftTempList";
    private static final int INSERT_BATCH_SIZE = 1000;
    private static final int FILE_BATCH_SIZE = 10000;
    private static final String TEMP_DIR = "/tmp/templist";
    // (tempTableName -> redisCacheKey)
    private static final ConcurrentMap<String, String> CACHE_LOOKUP = new ConcurrentHashMap<>();

    @Inject
    private RedshiftPartitionService redshiftPartitionService;

    @Inject
    private AthenaService athenaService;

    @Inject
    private PrestoConnectionService prestoConnectionService;

    @Inject
    private PrestoDbService prestoDbService;

    @Inject
    private S3Service s3Service;

    @Inject
    private RedisTemplate<String, Object> redisTemplate;

    @Inject
    private Configuration yarnConfiguration;

    @Value("${redshift.templist.maxsize}")
    private long maxSize;

    @Value("${aws.s3.data.stage.bucket}")
    private String dataStageBucket;

    @Override
    public String createTempListIfNotExists(ConcreteRestriction restriction, Class<?> fieldClz, //
                                            String sqlUser, String redshiftPartition) {
        String existingTempTable = getExistingTempTable(restriction, fieldClz, sqlUser, redshiftPartition);
        if (StringUtils.isNotBlank(existingTempTable)) {
            log.info("The temp list has already been created as {}.", existingTempTable);
            return existingTempTable;
        } else {
            if (restriction == null || !RestrictionUtils.isMultiValueOperator(restriction.getRelation()) //
                    || !(restriction.getRhs() instanceof CollectionLookup) //
                    || !(restriction.getLhs() instanceof AttributeLookup)) {
                throw new IllegalArgumentException("Not a valid big list restriction." + JsonUtils.serialize(restriction));
            }
            String lockName = getLockName(restriction, fieldClz, sqlUser, redshiftPartition);
            try {
                LockManager.registerCrossDivisionLock(lockName);
                LockManager.acquireWriteLock(lockName, 10, TimeUnit.MINUTES);
                log.info("Won the distributed lock {}", lockName);
            } catch (Exception e) {
                log.warn("Error while acquiring zk lock {}", lockName, e);
            }
            try {
                return createTempListInMutex(restriction, fieldClz, sqlUser, redshiftPartition);
            } finally {
                LockManager.releaseWriteLock(lockName);
            }
        }
    }


    private String createTempListInMutex(ConcreteRestriction restriction, Class<?> fieldClz, //
                                         String sqlUser, String redshiftPartition) {
        String existingTempTable = getExistingTempTable(restriction, fieldClz, sqlUser, redshiftPartition);
        if (StringUtils.isNotBlank(existingTempTable)) {
            log.info("The temp list has already been saved as {}} in redshift.", existingTempTable);
            return existingTempTable;
        }
        String tempTableName = TempListUtils.newTempTableName();
        try (PerformanceTimer timer = new PerformanceTimer("Create temp table " + tempTableName)) {
            CollectionLookup collectionLookup = (CollectionLookup) restriction.getRhs();
            int size = CollectionUtils.size(collectionLookup.getValues());
            if (size <= 0 || size > maxSize) {
                throw new IllegalArgumentException("Templist with " + size + " items is not allowed");
            }

            timer.setTimerMessage("Create temp table " + tempTableName + " for a list of " //
                    + CollectionUtils.size(collectionLookup.getValues()) + " values.");
            switch (sqlUser) {
                case PRESTO_USER:
                    createTempListInPrestoDb(collectionLookup, fieldClz, tempTableName);
                    break;
                case ATHENA_USER:
                    createTempListInAthena(collectionLookup, fieldClz, tempTableName);
                    break;
                default:
                    createTempListInRedshift(collectionLookup, fieldClz, tempTableName, redshiftPartition);
            }

            String checksum = TempListUtils.getCheckSum(restriction, fieldClz);
            String cacheKey = toCacheKey(checksum, sqlUser, redshiftPartition);
            redisTemplate.opsForValue().set(cacheKey, tempTableName, 1, TimeUnit.HOURS);
            CACHE_LOOKUP.put(tempTableName, cacheKey);

            return tempTableName;
        }
    }

    private void createTempListInPrestoDb(CollectionLookup collectionLookup, Class<?> fieldClz, String tempTableName) {
        String tableDir = writeTempListToLocal(collectionLookup, fieldClz, tempTableName);
        try {
            String hdfsDir = "/templist" + tempTableName;
            if (HdfsUtils.fileExists(yarnConfiguration, hdfsDir)) {
                HdfsUtils.rmdir(yarnConfiguration, hdfsDir);
            }
            HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, tableDir, hdfsDir);
            prestoDbService.deleteTableIfExists(tempTableName);
            prestoDbService.createTableIfNotExists(tempTableName, hdfsDir, DataUnit.DataFormat.AVRO);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            FileUtils.deleteQuietly(new File(tableDir));
        }
    }

    private void createTempListInAthena(CollectionLookup collectionLookup, Class<?> fieldClz, String tempTableName) {
        String tableDir = writeTempListToLocal(collectionLookup, fieldClz, tempTableName);
        try {
            String prefix = "templist/" + tempTableName;
            if (s3Service.isNonEmptyDirectory(dataStageBucket, prefix)) {
                s3Service.cleanupDirectory(dataStageBucket, prefix);
            }
            s3Service.uploadLocalDirectory(dataStageBucket, prefix, tableDir, true);
            athenaService.deleteTableIfExists(tempTableName);
            athenaService.createTableIfNotExists(tempTableName, dataStageBucket, prefix, DataUnit.DataFormat.AVRO);
        } finally {
            FileUtils.deleteQuietly(new File(tableDir));
        }
    }

    private String writeTempListToLocal(CollectionLookup collectionLookup, Class<?> fieldClz, String tempTableName) {
        String tableDir = TEMP_DIR + File.separator + tempTableName;
        File dirFile = new File(tableDir);
        try {
            if (dirFile.exists()) {
                FileUtils.deleteQuietly(dirFile);
            }
            log.info("Creating local directory " + tableDir);
            FileUtils.forceMkdir(dirFile);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create templist local dir.", e);
        }
        Preconditions.checkArgument(dirFile.isDirectory());

        String attrName = TempListUtils.VALUE_COLUMN;
        Schema schema = getSchema(tempTableName, attrName, fieldClz);
        List<GenericRecord> batch = new ArrayList<>();
        int split = 1;
        for (Object val: collectionLookup.getValues()) {
            GenericRecord record = new GenericRecordBuilder(schema) //
                    .set(attrName, TempListUtils.castVal(fieldClz, val)).build();
            batch.add(record);
            if (batch.size() >= FILE_BATCH_SIZE) {
                try {
                    String filePath = tableDir + File.separator + String.format("part-%03d", split++) + ".avro";
                    AvroUtils.writeToLocalFile(schema, batch, filePath, true);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to write local avro file", e);
                }
            }
        }
        if (!batch.isEmpty()) {
            try {
                String filePath = tableDir + File.separator + String.format("%03d", split) + ".avro";
                AvroUtils.writeToLocalFile(schema, batch, filePath, true);
            } catch (IOException e) {
                throw new RuntimeException("Failed to write local avro file", e);
            }
        }
        return tableDir;
    }

    private void createTempListInRedshift(CollectionLookup collectionLookup, Class<?> fieldClz, //
                                          String tempTableName, String redshiftPartition) {
        String attrName = TempListUtils.VALUE_COLUMN;
        RedshiftService redshiftService = getRedshiftService(redshiftPartition);

        String stagingTableName = "staging_" + tempTableName;
        redshiftService.dropTable(stagingTableName);

        RedshiftTableConfiguration tableConfig = getTableConfig(stagingTableName);
        Schema schema = getSchema(stagingTableName, attrName, fieldClz);
        redshiftService.createTable(tableConfig, schema);

        List<Object> batch = new ArrayList<>();
        for (Object val: collectionLookup.getValues()) {
            batch.add(val);
            if (batch.size() >= INSERT_BATCH_SIZE) {
                redshiftService.insertValuesIntoTable(stagingTableName,
                        Collections.singletonList(Pair.of(attrName, fieldClz)),
                        TempListUtils.insertVals(fieldClz, batch));
                log.info("Inserted {} values into {}", batch.size(), stagingTableName);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            redshiftService.insertValuesIntoTable(stagingTableName,
                    Collections.singletonList(Pair.of(attrName, fieldClz)),
                    TempListUtils.insertVals(fieldClz, batch));
            log.info("Inserted {} values into {}", batch.size(), stagingTableName);
            batch.clear();
        }

        redshiftService.dropTable(tempTableName);
        redshiftService.renameTable(stagingTableName, tempTableName);
    }

    @VisibleForTesting
    public void dropTempList(String tempTableName) {
        String partition = redshiftPartitionService.getDefaultPartition();
        String sqlUser = "";
        if (CACHE_LOOKUP.containsKey(tempTableName)) {
            String cacheKey = CACHE_LOOKUP.get(tempTableName);
            String[] tokens = cacheKey.split("\\|");
            sqlUser = tokens[1];
            partition = tokens[2];
            redisTemplate.delete(cacheKey);
            CACHE_LOOKUP.remove(tempTableName);
        }
        switch (sqlUser) {
            case PRESTO_USER:
                prestoDbService.deleteTableIfExists(tempTableName);
                break;
            case ATHENA_USER:
                athenaService.deleteTableIfExists(tempTableName);
                break;
            default:
                getRedshiftService(partition).dropTable(tempTableName);
        }
    }

    @PreDestroy
    public void preDestroy() {
        if (BeanFactoryEnvironment.Environment.TestClient.equals(BeanFactoryEnvironment.getEnvironment())) {
            for (String tempTableName : new ArrayList<>(CACHE_LOOKUP.keySet())) {
                dropTempList(tempTableName);
            }
        }
    }

    @VisibleForTesting
    String getExistingTempTable(ConcreteRestriction restriction, Class<?> fieldClz, String sqlUser, String redshiftPartition) {
        String checksum = TempListUtils.getCheckSum(restriction, fieldClz);
        String cacheKey = toCacheKey(checksum, sqlUser, redshiftPartition);
        Object obj = redisTemplate.opsForValue().get(cacheKey);
        if (obj != null) {
            String cacheTableName = (String) obj;
            RedshiftService redshiftService = getRedshiftService(redshiftPartition);
            if (!CACHE_LOOKUP.containsKey(cacheTableName) && redshiftService.hasTable(cacheTableName)) {
                CACHE_LOOKUP.put(cacheTableName, cacheKey);
            }
            if (CACHE_LOOKUP.containsKey(cacheTableName)) {
                return cacheTableName;
            }
        }
        return null;
    }

    private RedshiftTableConfiguration getTableConfig(String tableName) {
        RedshiftTableConfiguration tableConfig = new RedshiftTableConfiguration();
        tableConfig.setDistStyle(RedshiftTableConfiguration.DistStyle.All);
        tableConfig.setTableName(tableName);
        return tableConfig;
    }

    private Schema getSchema(String tableName, String attrName, Class<?> fieldClz) {
        return AvroUtils.constructSchema(tableName, Collections.singletonList(Pair.of(attrName, fieldClz)));
    }

    private String toCacheKey(String checksum, String sqlUser, String partition) {
        if (PRESTO_USER.equals(sqlUser)) {
            partition = prestoConnectionService.getClusterId();
        }
        return CACHE_PREFIX + "|" + sqlUser + "|" + partition + "|" + checksum;
    }

    private RedshiftService getRedshiftService(String partition) {
        return redshiftPartitionService.getBatchUserService(partition);
    }

    private String getLockName(ConcreteRestriction restriction, Class<?> fieldClz, String sqlUser, String redshiftPartition) {
        String checksum = TempListUtils.getCheckSum(restriction, fieldClz);
        if (ATHENA_USER.equals(sqlUser)) {
            return "TempList_" + sqlUser + "_" + checksum;
        } else {
            if (PRESTO_USER.equals(sqlUser)) {
                redshiftPartition = prestoConnectionService.getClusterId();
            }
            return "TempList_" + sqlUser + "_" + redshiftPartition + "_" + checksum;
        }
    }

}
