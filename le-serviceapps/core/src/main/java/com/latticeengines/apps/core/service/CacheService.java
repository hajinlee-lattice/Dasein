package com.latticeengines.apps.core.service;

import java.util.List;
import java.util.concurrent.Future;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.pls.EntityListCache;
import com.latticeengines.domain.exposed.security.Tenant;

public interface CacheService<T extends HasId<String>> {

    T getEntityById(String id);

    List<T> getEntitiesByTenant(@NotNull Tenant tenant);

    void deleteIdsByTenant(@NotNull Tenant tenant);

    void deleteIdsAndEntitiesByTenant(@NotNull Tenant tenant);

    void setIdsAndEntitiesByTenant(Tenant tenant, List<T> entities);

    List<T> getEntitiesByIds(List<String> ids);

    void deleteEntitiesByIds(List<String> ids);

    EntityListCache<T> getEntitiesAndNonExistEntitityIdsByTenant(Tenant tenant);

    Future<?> buildEntitiesCache(Tenant tenant);

    Future<?> clearModelSummaryCache(Tenant tenant, String entityId);
}
