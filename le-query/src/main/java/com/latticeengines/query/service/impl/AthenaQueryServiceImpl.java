package com.latticeengines.query.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.prestodb.exposed.service.AthenaService;
import com.latticeengines.query.exposed.service.AthenaQueryService;
import com.querydsl.sql.SQLQuery;

@Service
public class AthenaQueryServiceImpl implements AthenaQueryService {

    @Inject
    private AthenaService athenaService;

    @Override
    public long getCount(SQLQuery<?> sqlQuery) {
        sqlQuery.setUseLiterals(true);
        String sql = sqlQuery.getSQL().getSQL();
        if (sql.startsWith("select")) {
            sql = sql.substring(sql.indexOf("from"));
            sql = "select count(1) " + sql;
        } else if (sql.startsWith("with")) {
            // replace the last select statement
            String prefix = sql.substring(0, sql.lastIndexOf("select"));
            String finalSql = sql.substring(sql.lastIndexOf("select"));
            finalSql = finalSql.substring(finalSql.indexOf("from"));
            finalSql = "select count(1) " + finalSql;
            sql = prefix + finalSql;
        }
        return athenaService.queryObject(sql, Long.class);
    }

}
