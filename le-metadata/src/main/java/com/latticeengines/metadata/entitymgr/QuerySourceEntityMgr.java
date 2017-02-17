package com.latticeengines.metadata.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.metadata.QuerySource;

public interface QuerySourceEntityMgr extends BaseEntityMgr<QuerySource> {
    QuerySource createQuerySource(List<String> tableNames, String statisticsId, boolean isDefault);

    QuerySource getDefaultQuerySource();

    QuerySource getQuerySource(String name);

    void removeQuerySource(String name);
}
