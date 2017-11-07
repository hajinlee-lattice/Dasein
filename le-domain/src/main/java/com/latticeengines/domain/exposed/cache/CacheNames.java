package com.latticeengines.domain.exposed.cache;

public enum CacheNames {

    PLSCache, //
    AttributeRepoCache, //
    DataLakeCMCache, //
    DataLakeStatsCache, //
    EntityCountCache, //
    EntityDataCache, //
    EntityRatingCountCache, //
    JobsCache, //
    MetadataCache, //
    SessionCache;//

    public static CacheNames[] getCdlConsolidateCacheGroup() {
        return new CacheNames[] { EntityDataCache, EntityCountCache, EntityRatingCountCache };
    }

    public static CacheNames[] getCdlProfileCacheGroup() {
        return new CacheNames[] { AttributeRepoCache, DataLakeStatsCache, DataLakeCMCache, EntityCountCache,
                EntityDataCache,
                EntityRatingCountCache };
    }
}
