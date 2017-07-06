package com.latticeengines.datacloud.etl.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.etl.dao.SourceColumnDao;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;

@Component("sourceColumnEntityMgr")
public class SourceColumnEntityMgrImpl implements SourceColumnEntityMgr {

    @Autowired
    private SourceColumnDao sourceColumnDao;

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<SourceColumn> getSourceColumns(String sourceName) {
        return sourceColumnDao.getColumnsOfSource(sourceName);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public String[] generateCreateTableSqlStatements(DerivedSource source, String tableName) {
        List<SourceColumn> columns = sourceColumnDao.getColumnsOfSource(source.getSourceName());
        List<SourceColumn> groupByColumns = filterColumnsByCalculation(columns, SourceColumn.Calculation.GROUPBY, true);
        List<String> statements = new ArrayList<>();
        statements.add(toCreateTableSql(groupByColumns, tableName));
        for (SourceColumn column : filterColumnsByCalculation(columns, SourceColumn.Calculation.GROUPBY, false)) {
            statements.add(toAddColumnSql(column, tableName));
        }
        return statements.toArray(new String[statements.size()]);
    }

    private static List<SourceColumn> filterColumnsByCalculation(List<SourceColumn> columns, SourceColumn.Calculation calculation,
                                                                 boolean include) {
        List<SourceColumn> toReturn = new ArrayList<>();
        for (SourceColumn column : columns) {
            if (include && calculation.equals(column.getCalculation())) {
                toReturn.add(column);
            } else if (!include && !calculation.equals(column.getCalculation())) {
                toReturn.add(column);
            }
        }
        return toReturn;
    }

    private static String toCreateTableSql(List<SourceColumn> columns, String tableName) {
        String sql = "CREATE TABLE [" + tableName + "] (\n";
        List<String> lines = new ArrayList<>();
        for (SourceColumn column : columns) {
            lines.add(String.format("[%s] %s", column.getColumnName(), column.getColumnType()));
        }
        sql += StringUtils.join(lines, ",\n");
        sql += ");\n";
        return sql;
    }

    private static String toAddColumnSql(SourceColumn column, String tableName) {
        return String.format("ALTER TABLE [%s] ADD [%s] %s;\n", tableName, column.getColumnName(),
                column.getColumnType());
    }

}
