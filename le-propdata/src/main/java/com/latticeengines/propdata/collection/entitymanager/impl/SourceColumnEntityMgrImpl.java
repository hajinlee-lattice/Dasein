package com.latticeengines.propdata.collection.entitymanager.impl;

import static com.latticeengines.domain.exposed.propdata.collection.SourceColumn.Calculation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.collection.SourceColumn;
import com.latticeengines.propdata.collection.dao.SourceColumnDao;
import com.latticeengines.propdata.collection.entitymanager.SourceColumnEntityMgr;
import com.latticeengines.propdata.collection.source.ServingSource;

@Component("sourceColumnEntityMgr")
public class SourceColumnEntityMgrImpl implements SourceColumnEntityMgr {

    @Autowired
    private SourceColumnDao sourceColumnDao;

    private static Set<Calculation> unconfigurableCalculations =
            new HashSet<>(Arrays.asList(Calculation.GROUPBY, Calculation.OTHER));

    @Override
    @Transactional(value = "propDataCollection", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<SourceColumn> getConfigurableColumns(ServingSource source) {
        List<SourceColumn> columns = sourceColumnDao.getColumnsOfSource(source);
        return filterConfigurableColumns(columns);
    }

    @Override
    @Transactional(value = "propDataCollection", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public String[] generateCreateTableSqlStatements(ServingSource source, String tableName) {
        List<SourceColumn> columns = sourceColumnDao.getColumnsOfSource(source);
        List<SourceColumn> groupByColumns = filterColumnsByCalculation(columns, Calculation.GROUPBY);
        List<String> statements = new ArrayList<>();
        statements.add(toCreateTableSql(groupByColumns, tableName));
        for (SourceColumn column: filterConfigurableColumns(columns)) {
            statements.add(toAddColumnSql(column, tableName));
        }
        for (SourceColumn column: filterColumnsByCalculation(columns, Calculation.OTHER)) {
            statements.add(toAddColumnSql(column, tableName));
        }
        return statements.toArray(new String[statements.size()]);
    }

    private static List<SourceColumn> filterConfigurableColumns(List<SourceColumn> columns) {
        List<SourceColumn> toReturn = new ArrayList<>();
        for (SourceColumn column: columns) {
            if (!unconfigurableCalculations.contains(column.getCalculation())) {
                toReturn.add(column);
            }
        }
        return toReturn;
    }

    private static List<SourceColumn> filterColumnsByCalculation(List<SourceColumn> columns, Calculation calculation) {
        List<SourceColumn> toReturn = new ArrayList<>();
        for (SourceColumn column: columns) {
            if (calculation.equals(column.getCalculation())) {
                toReturn.add(column);
            }
        }
        return toReturn;
    }

    private static String toCreateTableSql(List<SourceColumn> columns, String tableName) {
        String sql = "CREATE TABLE [" + tableName + "] (\n";
        List<String> lines = new ArrayList<>();
        for (SourceColumn column: columns) {
            lines.add(String.format("[%s] %s", column.getColumnName(), column.getColumnType()));
        }
        sql += StringUtils.join(lines, ",\n");
        sql += ");\n";
        return sql;
    }

    private static String toAddColumnSql(SourceColumn column, String tableName) {
       return String.format("ALTER TABLE [%s] ADD [%s] %s;\n",
                tableName, column.getColumnName(), column.getColumnType());
    }

}
