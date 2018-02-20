package com.latticeengines.domain.exposed.cache;

public enum CacheName {

    PLSCache(Constants.PLSCacheName), //
    DataLakeCMCache(Constants.DataLakeCMCacheName), //
    DataLakeTopNTreeCache(Constants.DataLakeTopNTreeCache), //
    DataLakeStatsCubesCache(Constants.DataLakeStatsCubesCache), //
    EntityCountCache(Constants.EntityCountCacheName), //
    EntityDataCache(Constants.EntityDataCacheName), //
    EntityRatingCountCache(Constants.EntityRatingCountCacheName), //
    RatingDataCache(Constants.RatingDataCacheName), //
    RatingCoverageCache(Constants.RatingCoverageCacheName), //
    RatingSummariesCache(Constants.RatingSummariesCacheName), //
    AttrRepoCache(Constants.AttrRepoCacheName), //
    TimeTranslatorCache(Constants.TimeTranslatorCacheName), //
    JobsCache(Constants.JobsCacheName), //
    MetadataCache(Constants.MetadataCacheName), //

    DataCloudCMCache(Constants.DataCloudCMCacheName), //
    DataCloudStatsCache(Constants.DataCloudStatsCacheName), //
    DataCloudVersionCache(Constants.DataCloudVersionCacheName), //

    SessionCache(Constants.SessionCacheName);//

    private String name;

    CacheName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public static CacheName[] getDataCloudCacheGroup() {
        return new CacheName[] { DataCloudVersionCache };
    }

    public static CacheName[] getDataCloudLocalCacheGroup() {
        return new CacheName[] { DataCloudCMCache, DataCloudStatsCache };
    }

    public static CacheName[] getCdlCacheGroup() {
        return new CacheName[] { //
                DataLakeStatsCubesCache, //
                DataLakeTopNTreeCache, //
                DataLakeCMCache, //
                EntityCountCache, //
                EntityDataCache, //
                EntityRatingCountCache, //
                RatingCoverageCache, //
                RatingSummariesCache //
        };
    }

    public static CacheName[] getCdlLocalCacheGroup() {
        return new CacheName[] { TimeTranslatorCache };
    }

    public static CacheName[] getRatingEnginesCacheGroup() {
        return new CacheName[] { RatingSummariesCache };
    }

    public static class Constants {
        public static final String PLSCacheName = "PLSCache";
        public static final String DataLakeTopNTreeCache = "DataLakeTopNTreeCache";
        public static final String DataLakeCMCacheName = "DataLakeCMCache";
        public static final String DataLakeStatsCubesCache = "DataLakeStatsCubesCache";
        public static final String EntityCountCacheName = "EntityCountCache";
        public static final String EntityDataCacheName = "EntityDataCache";
        public static final String EntityRatingCountCacheName = "EntityRatingCountCache";
        public static final String RatingDataCacheName = "RatingDataCache";
        public static final String RatingCoverageCacheName = "RatingCoverageCache";
        public static final String RatingSummariesCacheName = "RatingSummariesCache";
        public static final String JobsCacheName = "JobsCache";
        public static final String MetadataCacheName = "MetadataCache";
        public static final String SessionCacheName = "SessionCache";
        public static final String AttrRepoCacheName = "AttrRepoCache";
        public static final String TimeTranslatorCacheName = "TimeTranslatorCache";
        public static final String DataCloudCMCacheName = "DataCloudCMCache";
        public static final String DataCloudStatsCacheName = "DataCloudStatsCacheName";
        public static final String DataCloudVersionCacheName = "DataCloudVersionCache";
    }
}
