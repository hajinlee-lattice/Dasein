package com.latticeengines.domain.exposed.cache;

public enum CacheName {

    PLSCache(Constants.PLSCacheName), //
    DataLakeCMCache(Constants.DataLakeCMCacheName), //
    DataLakeStatsCache(Constants.DataLakeStatsCacheName), //
    EntityCountCache(Constants.EntityCountCacheName), //
    EntityDataCache(Constants.EntityDataCacheName), //
    EntityRatingCountCache(Constants.EntityRatingCountCacheName), //
    AttrRepoCache(Constants.AttrRepoCacheName), //
    JobsCache(Constants.JobsCacheName), //
    MetadataCache(Constants.MetadataCacheName), //
    SessionCache(Constants.SessionCacheName);//

    private String name;

    CacheName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public static CacheName[] getCdlConsolidateCacheGroup() {
        return new CacheName[] { EntityDataCache, EntityCountCache, EntityRatingCountCache };
    }

    public static CacheName[] getCdlProfileCacheGroup() {
        return new CacheName[] { DataLakeStatsCache, DataLakeCMCache, EntityCountCache, EntityDataCache,
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
        public static final String AttrRepoCacheName = "AttrRepoCache";
    }
}
