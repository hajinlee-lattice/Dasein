package com.latticeengines.metadata.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.QuerySource;

public interface QuerySourceService {

    List<QuerySource> getQuerySources(String customerSpace);

    QuerySource getDefaultQuerySource(String customerSpace);

    QuerySource createDefaultQuerySource(String customerSpace, String statisticsId, List<String> tableNames);

    QuerySource getQuerySource(String customerSpace, String querySourceName);

    QuerySource createQuerySource(String customerSpace, String statisticsId, List<String> tableNames);
}
