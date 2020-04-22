package com.latticeengines.objectapi.service.sparksql.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.query.CollectionLookup;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.TempListUtils;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.objectapi.service.TempListService;
import com.latticeengines.query.exposed.service.SparkSQLService;

@Service("tempListServiceSparkSQL")
public class TempListServiceSparkSQLImpl implements TempListService {

    private static final Logger log = LoggerFactory.getLogger(TempListServiceSparkSQLImpl.class);

    // checksum -> table name
    private final Map<String, String> tempListTables = new HashMap<>();

    @Inject
    private SparkSQLService sparkSQLService;

    @Value("${redshift.templist.maxsize}")
    private long maxSize;

    private LivySession livySession;

    @Override
    public String createTempListIfNotExists(ConcreteRestriction restriction, Class<?> fieldClz, String redshiftPartition) {
        String checksum = TempListUtils.getCheckSum(restriction, fieldClz);
        String tempTableName;
        if (tempListTables.containsKey(checksum)) {
            tempTableName = tempListTables.get(checksum);
        } else {
            tempTableName = createTempListTable(restriction, fieldClz);
            tempListTables.put(checksum, tempTableName);
        }
        return tempTableName;
    }

    private String createTempListTable(ConcreteRestriction restriction, Class<?> fieldClz) {
        String tableName = TempListUtils.newShortTempTableName();
        CollectionLookup collectionLookup = (CollectionLookup) restriction.getRhs();
        int size = CollectionUtils.size(collectionLookup.getValues());
        if (size > maxSize) {
            throw new IllegalArgumentException("Templist with " + size + " items is not allowed");
        }
        List<List<Object>> vals = TempListUtils.insertVals(fieldClz, collectionLookup.getValues());
        sparkSQLService.createTempListView(livySession, tableName, fieldClz, vals);
        return tableName;
    }

    void setLivySession(LivySession livySession) {
        tempListTables.clear();
        log.info("Clear tempListTables.");
        if (livySession != null) {
            log.info("Link " + getClass().getSimpleName() + " to livy session " //
                    + livySession.getSessionId() + " [" + livySession.getAppId() + "]");
        } else if (this.livySession != null){
            log.info("Detach " + getClass().getSimpleName() + " from livy session " //
                    + this.livySession.getSessionId() + " [" + this.livySession.getAppId() + "]");
        }
        this.livySession = livySession;
    }

}
