package com.latticeengines.query.exposed.service;

import com.querydsl.sql.SQLQuery;

public interface AthenaQueryService {

    long getCount(SQLQuery<?> sqlQuery);

}
