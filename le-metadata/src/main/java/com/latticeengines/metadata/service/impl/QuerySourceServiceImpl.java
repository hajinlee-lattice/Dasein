package com.latticeengines.metadata.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.QuerySource;
import com.latticeengines.metadata.entitymgr.QuerySourceEntityMgr;
import com.latticeengines.metadata.service.QuerySourceService;

@Component("querySourceService")
public class QuerySourceServiceImpl implements QuerySourceService {
    @Autowired
    private QuerySourceEntityMgr querySourceEntityMgr;

    @Override
    public List<QuerySource> getQuerySources(String customerSpace) {
        return querySourceEntityMgr.findAll();
    }

    @Override
    public QuerySource getDefaultQuerySource(String customerSpace) {
        return querySourceEntityMgr.getDefaultQuerySource();
    }

    @Override
    public QuerySource createDefaultQuerySource(String customerSpace, String statisticsId, List<String> tableNames) {
        return querySourceEntityMgr.createQuerySource(tableNames, statisticsId, true);
    }

    @Override
    public QuerySource getQuerySource(String customerSpace, String querySourceName) {
        return querySourceEntityMgr.getQuerySource(querySourceName);
    }

    @Override
    public QuerySource createQuerySource(String customerSpace, String statisticsId, List<String> tableNames) {
        return querySourceEntityMgr.createQuerySource(tableNames, statisticsId, false);
    }
}
