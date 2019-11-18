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
    ServingMetadataLocalCache(Constants.ServingMetadataLocalCacheName), //
    TableRoleMetadataCache(Constants.TableRoleMetadataCacheName), //
    DantePreviewTokenCache(Constants.DantePreviewTokenCacheName), //

    DataCloudCMCache(Constants.DataCloudCMCacheName), //
    DataCloudStatsCache(Constants.DataCloudStatsCacheName), //
    DataCloudVersionCache(Constants.DataCloudVersionCacheName), //

    EMRClusterCache(Constants.EMRClusterCacheName), //

    ActiveStackInfoCache(Constants.ActiveStackInfoCacheName), //

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
                TableRoleMetadataCache, //
                ServingMetadataLocalCache, //
                AttrRepoCache, //
                DataLakeCMCache, //
                DataLakeStatsCubesCache, //
                DataLakeTopNTreeCache, //
                ObjectApiCache, //
                ServingMetadataCache, //
                TimeTranslatorCache };
    }

    // should all key-ed by tenantId|entity|*
    public static CacheName[] getCdlServingCacheGroup() {
        return new CacheName[] {
                ServingMetadataCache, //
                ServingMetadataLocalCache, //
                DataLakeCMCache //
        };
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
        public static final String JobsCacheName = "JobsCache";
        public static final String MetadataCacheName = "MetadataCache";
        public static final String SessionCacheName = "SessionCache";
        public static final String AttrRepoCacheName = "AttrRepoCache";
        public static final String TimeTranslatorCacheName = "TimeTranslatorCache";
        public static final String DataCloudCMCacheName = "DataCloudCMCache";
        public static final String DataCloudStatsCacheName = "DataCloudStatsCache";
        public static final String DataCloudVersionCacheName = "DataCloudVersionCache";
        public static final String ServingMetadataCacheName = "ServingMetadataCache";
        public static final String ServingMetadataLocalCacheName = "ServingMetadataLocalCache";
        public static final String TableRoleMetadataCacheName = "TableRoleMetadataCache";
        public static final String DantePreviewTokenCacheName = "DantePreviewTokenCache";
        public static final String EMRClusterCacheName = "EMRClusterCache";
        public static final String ModelSummaryCacheName = "ModelSummaryCache";
        public static final String ActiveStackInfoCacheName = "ActiveStackInfoCache";
        public static final String LastActionTimeCacheName = "LastActionTimeCache";
        public static final String PAFailCountCacheName = "PAFailCountCache";
    }
}
