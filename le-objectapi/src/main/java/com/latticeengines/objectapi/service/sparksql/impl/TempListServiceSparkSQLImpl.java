package com.latticeengines.objectapi.service.sparksql.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.query.CollectionLookup;
import com.latticeengines.domain.exposed.query.ConcreteRestriction;
import com.latticeengines.domain.exposed.query.TempListUtils;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.objectapi.service.TempListService;
import com.latticeengines.query.exposed.service.SparkSQLService;

@Service("tempListServiceSparkSQL")
public class TempListServiceSparkSQLImpl implements TempListService {

    // checksum -> table name
    private final Map<String, String> tempListTables = new HashMap<>();

    @Inject
    private SparkSQLService sparkSQLService;

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
        List<List<Object>> vals = TempListUtils.insertVals(fieldClz, collectionLookup.getValues());
        sparkSQLService.createTempListView(livySession, tableName, fieldClz, vals);
        return tableName;
    }

    void setLivySession(LivySession livySession) {
        this.livySession = livySession;
    }

}
