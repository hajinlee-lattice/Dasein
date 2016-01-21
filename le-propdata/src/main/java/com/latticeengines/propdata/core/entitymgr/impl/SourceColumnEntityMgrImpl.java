package com.latticeengines.propdata.core.entitymgr.impl;

import static com.latticeengines.domain.exposed.propdata.manage.SourceColumn.Calculation;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;
import com.latticeengines.propdata.core.dao.SourceColumnDao;
import com.latticeengines.propdata.core.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.propdata.core.source.DerivedSource;

@Component("sourceColumnEntityMgr")
public class SourceColumnEntityMgrImpl implements SourceColumnEntityMgr {

    @Autowired
    private SourceColumnDao sourceColumnDao;

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<SourceColumn> getSourceColumns(DerivedSource source) {
        return sourceColumnDao.getColumnsOfSource(source);
    }

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public String[] generateCreateTableSqlStatements(DerivedSource source, String tableName) {
        List<SourceColumn> columns = sourceColumnDao.getColumnsOfSource(source);
        List<SourceColumn> groupByColumns = filterColumnsByCalculation(columns, Calculation.GROUPBY, true);
        List<String> statements = new ArrayList<>();
        statements.add(toCreateTableSql(groupByColumns, tableName));
        for (SourceColumn column: filterColumnsByCalculation(columns, Calculation.GROUPBY, false)) {
            statements.add(toAddColumnSql(column, tableName));
        }
        List<String> groupedStatements = concatStatements(statements);
        return groupedStatements.toArray(new String[groupedStatements.size()]);
    }

    private List<String> concatStatements(List<String> statements) {
        List<String> groups = new ArrayList<>();
        String bigStatement = "";
        for (String statement: statements) {
            if (bigStatement.length() < 4000) {
                bigStatement += statement;
            } else {
                groups.add(bigStatement);
                bigStatement = "";
            }
        }
        if (StringUtils.isNotEmpty(bigStatement)) {
            groups.add(bigStatement);
        }
        return groups;
    }

    private static List<SourceColumn> filterColumnsByCalculation(List<SourceColumn> columns, Calculation calculation,
                                                                 boolean include) {
        List<SourceColumn> toReturn = new ArrayList<>();
        for (SourceColumn column: columns) {
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
