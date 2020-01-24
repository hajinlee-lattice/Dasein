package com.latticeengines.domain.exposed.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

public final class PeriodStrategyUtils {

    protected PeriodStrategyUtils() {
        throw new UnsupportedOperationException();
    }

    private static final String STRATEGY = "Strategy";

    public static String getTablePrefixFromPeriodStrategy(PeriodStrategy strategy) {
        return STRATEGY + strategy.getName();
    }

    public static String getTablePrefixFromPeriodStrategyName(String periodStrategyName) {
        return STRATEGY + periodStrategyName;
    }

    public static String getPeriodStrategyNameFromPeriodTableName(String tableName,
            TableRoleInCollection role) {
        return tableName.substring(STRATEGY.length(), tableName.indexOf(role.name()));
    }

    public static Table findPeriodTableFromStrategy(List<Table> periodTables,
            PeriodStrategy strategy) {
        for (Table table : periodTables) {
            if (table.getName().startsWith(getTablePrefixFromPeriodStrategy(strategy))) {
                return table;
            }
        }
        throw new RuntimeException(
                "Fail to find period table from period strategy " + strategy.getName());
    }

    public static Table findPeriodTableFromStrategyName(List<Table> periodTables,
            String strategyName) {
        for (Table table : periodTables) {
            if (table.getName().startsWith(getTablePrefixFromPeriodStrategyName(strategyName))) {
                return table;
            }
        }
        throw new RuntimeException(
                "Fail to find period table from period strategy named " + strategyName);
    }

    public static List<String> filterPeriodTablesByPeriods(List<String> periodTables,
            Set<String> periods) {
        List<String> filtered = new ArrayList<>();
        Set<String> tablePrefixes = new HashSet<>();
        for (String period : periods) {
            if (period.equalsIgnoreCase(PeriodStrategy.Template.Year.name())) {
                tablePrefixes.add(getTablePrefixFromPeriodStrategy(PeriodStrategy.CalendarYear));
            } else if (period.equalsIgnoreCase(PeriodStrategy.Template.Quarter.name())) {
                tablePrefixes.add(getTablePrefixFromPeriodStrategy(PeriodStrategy.CalendarQuarter));
            } else if (period.equalsIgnoreCase(PeriodStrategy.Template.Month.name())) {
                tablePrefixes.add(getTablePrefixFromPeriodStrategy(PeriodStrategy.CalendarMonth));
            } else if (period.equalsIgnoreCase(PeriodStrategy.Template.Week.name())) {
                tablePrefixes.add(getTablePrefixFromPeriodStrategy(PeriodStrategy.CalendarWeek));
            } else {
                throw new RuntimeException("Unknown period " + period);
            }
        }
        for (String periodTable : periodTables) {
            for (String tablePrefix : tablePrefixes) {
                if (periodTable.startsWith(tablePrefix)) {
                    filtered.add(periodTable);
                }
            }
        }
        return filtered;
    }
}
