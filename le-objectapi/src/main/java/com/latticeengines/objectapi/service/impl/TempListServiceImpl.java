package com.latticeengines.objectapi.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.camille.exposed.locks.LockManager;
import com.latticeengines.common.exposed.bean.BeanFactoryEnvironment;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.CollectionLookup;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.TempListUtils;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.util.RestrictionUtils;
import com.latticeengines.objectapi.service.TempListService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;

@Service("tempListService")
public class TempListServiceImpl implements TempListService {

    private static final Logger log = LoggerFactory.getLogger(TempListServiceImpl.class);

    private static final String CACHE_PREFIX = "RedShiftTempList";
    private static final int INSERT_BATCH_SIZE = 1000;
    // (tempTableName -> redisCacheKey)
    private static final ConcurrentMap<String, String> CACHE_LOOKUP = new ConcurrentHashMap<>();

    @Inject
    private RedshiftPartitionService redshiftPartitionService;

    @Inject
    private RedisTemplate<String, Object> redisTemplate;

    @Override
    public String createTempListIfNotExists(ConcreteRestriction restriction, Class<?> fieldClz, String redshiftPartition) {
        String existingTempTable = getExistingTempTable(restriction, fieldClz, redshiftPartition);
        if (StringUtils.isNotBlank(existingTempTable)) {
            log.info("The temp list has already been saved as {}} in redshift.", existingTempTable);
            return existingTempTable;
        } else {
            if (restriction == null || !RestrictionUtils.isMultiValueOperator(restriction.getRelation()) //
                    || !(restriction.getRhs() instanceof CollectionLookup) //
                    || !(restriction.getLhs() instanceof AttributeLookup)) {
                throw new IllegalArgumentException("Not a valid big list restriction." + JsonUtils.serialize(restriction));
            }
            String lockName = getLockName(restriction, fieldClz, redshiftPartition);
            try {
                LockManager.registerCrossDivisionLock(lockName);
                LockManager.acquireWriteLock(lockName, 10, TimeUnit.MINUTES);
                log.info("Won the distributed lock");
            } catch (Exception e) {
                log.warn("Error while acquiring zk lock {}", lockName, e);
            }
            try {
                return createTempListInMutex(restriction, fieldClz, redshiftPartition);
            } finally {
                LockManager.releaseWriteLock(lockName);
            }
        }
    }

    private String createTempListInMutex(ConcreteRestriction restriction, Class<?> fieldClz, String redshiftPartition) {
        String existingTempTable = getExistingTempTable(restriction, fieldClz, redshiftPartition);
        if (StringUtils.isNotBlank(existingTempTable)) {
            log.info("The temp list has already been saved as {}} in redshift.", existingTempTable);
            return existingTempTable;
        }
        String tempTableName = TempListUtils.newTempTableName();
        try (PerformanceTimer timer = new PerformanceTimer("Create temp table " + tempTableName)) {
            CollectionLookup collectionLookup = (CollectionLookup) restriction.getRhs();
            timer.setTimerMessage("Create temp table " + tempTableName + " for a list of " //
                    + CollectionUtils.size(collectionLookup.getValues()) + " values.");

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

            String checksum = TempListUtils.getCheckSum(restriction, fieldClz);
            String cacheKey = toCacheKey(checksum, redshiftPartition);
            redisTemplate.opsForValue().set(cacheKey, tempTableName, 1, TimeUnit.HOURS);
            CACHE_LOOKUP.put(tempTableName, cacheKey);

            return tempTableName;
        }
    }

    @VisibleForTesting
    public void dropTempList(String tempTableName) {
        String partition = redshiftPartitionService.getDefaultPartition();
        if (CACHE_LOOKUP.containsKey(tempTableName)) {
            String cacheKey = CACHE_LOOKUP.get(tempTableName);
            partition = cacheKey.split("\\|")[1];
            redisTemplate.delete(cacheKey);
            CACHE_LOOKUP.remove(tempTableName);
        }
        getRedshiftService(partition).dropTable(tempTableName);
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
    String getExistingTempTable(ConcreteRestriction restriction, Class<?> fieldClz, String redshiftPartition) {
        String checksum = TempListUtils.getCheckSum(restriction, fieldClz);
        String cacheKey = toCacheKey(checksum, redshiftPartition);
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

    private String toCacheKey(String checksum, String partition) {
        return CACHE_PREFIX + "|" + partition + "|" + checksum;
    }

    private RedshiftService getRedshiftService(String partition) {
        return redshiftPartitionService.getBatchUserService(partition);
    }

    private String getLockName(ConcreteRestriction restriction, Class<?> fieldClz, String redshiftPartition) {
        String checksum = TempListUtils.getCheckSum(restriction, fieldClz);
        return  "RedshiftTempList_" + redshiftPartition + "_" + checksum;
    }

}
