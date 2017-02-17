package com.latticeengines.network.exposed.metadata;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.QuerySource;

public interface QuerySourceInterface {
    List<QuerySource> getQuerySources(String customerSpace);

    QuerySource getDefaultQuerySource(String customerSpace);

    QuerySource createDefaultQuerySource(String customerSpace, String statisticsId, List<String> tableNames);

    QuerySource getQuerySource(String customerSpace, String querySourceName);

    QuerySource createQuerySource(String customerSpace, String statisticsId, List<String> tableNames);
}
