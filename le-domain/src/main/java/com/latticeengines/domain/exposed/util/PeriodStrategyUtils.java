package com.latticeengines.domain.exposed.util;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

public class PeriodStrategyUtils {

    private static final String STRATEGY = "Strategy";

    public static String getTablePrefixFromPeriodStrategy(PeriodStrategy strategy) {
        return STRATEGY + strategy.getName();
    }

    public static String getTablePrefixFromPeriodStrategyName(String periodStrategyName) {
        return STRATEGY + periodStrategyName;
    }

    public static String getPeriodStrategyNameFromPeriodTableName(String tableName, TableRoleInCollection role) {
        return tableName.substring(STRATEGY.length(), tableName.indexOf(role.name()));
    }

    public static Table findPeriodTableFromStrategy(List<Table> periodTables, PeriodStrategy strategy) {
        for (Table table : periodTables) {
            if (table.getName().startsWith(getTablePrefixFromPeriodStrategy(strategy))) {
                return table;
            }
        }
        throw new RuntimeException("Fail to find period table from period strategy " + strategy.getName());
    }
}
