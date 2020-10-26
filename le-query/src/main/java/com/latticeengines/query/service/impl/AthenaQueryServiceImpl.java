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
        sql = sql.substring(sql.indexOf("from"));
        sql = "select count(1) " + sql;
        return athenaService.queryObject(sql, Long.class);
    }

}
