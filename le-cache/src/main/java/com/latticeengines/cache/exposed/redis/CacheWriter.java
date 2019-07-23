package com.latticeengines.cache.exposed.redis;

import java.util.List;

import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.pls.EntityListCache;
import com.latticeengines.domain.exposed.security.Tenant;

public interface CacheWriter<T extends HasId<String>> {

    T getEntityById(String id);

    List<T> getEntitiesByTenant(Tenant tenant);

    void deleteIdsByTenant(Tenant tenant);

    void deleteIdsAndEntitiesByTenant(Tenant tenant);

    void setIdsAndEntitiesByTenant(Tenant tenant, List<T> entities);

    void setEntitiesIdsByTenant(Tenant tenant, List<T> entities);

    List<T> getEntitiesByIds(List<String> ids);

    void deleteEntitiesByIds(List<String> ids);

    void setEntities(List<T> entities);

    EntityListCache<T> getEntitiesAndNonExistEntitityIdsByTenant(Tenant tenant);

    String getCacheKeyForObject(String id);

    String getLockKeyForObject(Tenant tenant);

    void setEntitiesIdsAndNonExistEntitiesByTenant(Tenant tenant, List<T> entities);
}
