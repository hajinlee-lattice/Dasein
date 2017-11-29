package com.latticeengines.domain.exposed.cache;

public enum CacheNames {

    PLSCache(Constants.PLSCacheName), //
    DataLakeCMCache(Constants.DataLakeCMCacheName), //
    DataLakeStatsCache(Constants.DataLakeStatsCacheName), //
    EntityCountCache(Constants.EntityCountCacheName), //
    EntityDataCache(Constants.EntityDataCacheName), //
    EntityRatingCountCache(Constants.EntityRatingCountCacheName), //
    JobsCache(Constants.JobsCacheName), //
    MetadataCache(Constants.MetadataCacheName), //
    SessionCache(Constants.SessionCacheName);//

    private String name;

    CacheNames(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public static CacheNames[] getCdlConsolidateCacheGroup() {
        return new CacheNames[] { EntityDataCache, EntityCountCache, EntityRatingCountCache };
    }

    public static CacheNames[] getCdlProfileCacheGroup() {
        return new CacheNames[] { DataLakeStatsCache, DataLakeCMCache, EntityCountCache, EntityDataCache,
                EntityRatingCountCache };
    }

    public static class Constants {
        public static final String PLSCacheName = "PLSCache";
        public static final String DataLakeCMCacheName = "DataLakeCMCache";
        public static final String DataLakeStatsCacheName = "DataLakeStatsCache";
        public static final String EntityCountCacheName = "EntityCountCache";
        public static final String EntityDataCacheName = "EntityDataCache";
        public static final String EntityRatingCountCacheName = "EntityRatingCountCache";
        public static final String JobsCacheName = "JobsCache";
        public static final String MetadataCacheName = "MetadataCache";
        public static final String SessionCacheName = "SessionCache";
    }
}
