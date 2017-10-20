package com.latticeengines.domain.exposed.cache;

public enum CacheNames {

    SessionCache, //
    EntityCache, //
    MetadataCache, //
    JobsCache, //
    BucketedMetadataCache;

    public static void main(String[] args) {
        System.out.println(SessionCache.name());
    }
}
