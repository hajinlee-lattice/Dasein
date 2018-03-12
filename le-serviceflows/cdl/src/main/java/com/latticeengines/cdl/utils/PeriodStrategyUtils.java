package com.latticeengines.cdl.utils;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.Table;

public class PeriodStrategyUtils {
    public static String getTablePrefixFromPeriodStrategy(PeriodStrategy strategy) {
        return "Strategy" + strategy.getName();
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
