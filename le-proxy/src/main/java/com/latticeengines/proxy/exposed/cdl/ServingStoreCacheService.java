package com.latticeengines.proxy.exposed.cdl;

import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface ServingStoreCacheService {

    List<ColumnMetadata> getDecoratedMetadata(String customerSpace, BusinessEntity entity);

    Set<String> getServingTableColumns(String customerSpace, BusinessEntity entity);

    List<ColumnMetadata> getDateAttrs(String customerSpace, BusinessEntity entity);

    void clearCache(String customerSpace, BusinessEntity entity);
}
