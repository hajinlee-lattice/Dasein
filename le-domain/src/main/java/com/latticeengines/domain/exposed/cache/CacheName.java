package com.latticeengines.domain.exposed.cache;

public enum CacheName {

    PLSCache(Constants.PLSCacheName), //
    DataLakeCMCache(Constants.DataLakeCMCacheName), //
    DataLakeTopNTreeCache(Constants.DataLakeTopNTreeCache), //
    DataLakeStatsCubesCache(Constants.DataLakeStatsCubesCache), //
    ObjectApiCache(Constants.ObjectApiCacheName), //
    AttrRepoCache(Constants.AttrRepoCacheName), //
    TimeTranslatorCache(Constants.TimeTranslatorCacheName), //
    JobsCache(Constants.JobsCacheName), //
    MetadataCache(Constants.MetadataCacheName), //
    ServingMetadataCache(Constants.ServingMetadataCacheName), //
    TableRoleMetadataCache(Constants.TableRoleMetadataCacheName), //
    DantePreviewTokenCache(Constants.DantePreviewTokenCacheName), //

    DataCloudCMCache(Constants.DataCloudCMCacheName), //
    DataCloudStatsCache(Constants.DataCloudStatsCacheName), //
    DataCloudVersionCache(Constants.DataCloudVersionCacheName), //

    EMRClusterCache(Constants.EMRClusterCacheName), //

    SessionCache(Constants.SessionCacheName);//

    private String name;

    CacheName(String name) {
        this.name = name;
    }

    public static CacheName[] getDataCloudLocalCacheGroup() {
        return new CacheName[] { DataCloudVersionCache, DataCloudCMCache, DataCloudStatsCache };
    }

    public static CacheName[] getCdlCacheGroup() {
        return new CacheName[] { //
                DataLakeStatsCubesCache, //
                DataLakeTopNTreeCache, //
                DataLakeCMCache, //
                ObjectApiCache, //
                ServingMetadataCache, //
                TableRoleMetadataCache };
    }

    public static CacheName[] getCdlLocalCacheGroup() {
        return new CacheName[] { TimeTranslatorCache };
    }

    public String getName() {
        return this.name;
    }

    public static class Constants {
        public static final String PLSCacheName = "PLSCache";
        public static final String DataLakeTopNTreeCache = "DataLakeTopNTreeCache";
        public static final String DataLakeCMCacheName = "DataLakeCMCache";
        public static final String DataLakeStatsCubesCache = "DataLakeStatsCubesCache";
        public static final String ObjectApiCacheName = "ObjectApiCache";
        public static final String EntityCountCacheName = "EntityCountCache";
        public static final String EntityDataCacheName = "EntityDataCache";
        public static final String EntityRatingCountCacheName = "EntityRatingCountCache";
        public static final String RatingDataCacheName = "RatingDataCache";
        public static final String RatingCoverageCacheName = "RatingCoverageCache";
        public static final String JobsCacheName = "JobsCache";
        public static final String MetadataCacheName = "MetadataCache";
        public static final String SessionCacheName = "SessionCache";
        public static final String AttrRepoCacheName = "AttrRepoCache";
        public static final String TimeTranslatorCacheName = "TimeTranslatorCache";
        public static final String DataCloudCMCacheName = "DataCloudCMCache";
        public static final String DataCloudStatsCacheName = "DataCloudStatsCache";
        public static final String DataCloudVersionCacheName = "DataCloudVersionCache";
        public static final String ServingMetadataCacheName = "ServingMetadataCache";
        public static final String TableRoleMetadataCacheName = "TableRoleMetadataCache";
        public static final String DantePreviewTokenCacheName = "DantePreviewTokenCache";
        public static final String EMRClusterCacheName = "EMRClusterCache";
        public static final String CSVImportMapperCacheName = "CSVImportMapperCache";
        public static final String CDLScheduledJobCacheName = "CDLScheduledJobCache";
    }
}
