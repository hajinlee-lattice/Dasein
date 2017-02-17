package com.latticeengines.metadata.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.QuerySource;
import com.latticeengines.metadata.dao.QuerySourceDao;

@Component("querySourceDao")
public class QuerySourceDaoImpl extends BaseDaoImpl<QuerySource> implements QuerySourceDao {
    @Override
    protected Class<QuerySource> getEntityClass() {
        return QuerySource.class;
    }
}
