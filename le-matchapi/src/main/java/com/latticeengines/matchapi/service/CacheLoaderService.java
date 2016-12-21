package com.latticeengines.matchapi.service;


public interface CacheLoaderService<E> {

    void loadCache(CacheLoaderConfig config);

}
