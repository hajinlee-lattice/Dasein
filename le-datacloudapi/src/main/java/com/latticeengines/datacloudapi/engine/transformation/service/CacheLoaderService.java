package com.latticeengines.datacloudapi.engine.transformation.service;


public interface CacheLoaderService<E> {

    void loadCache(CacheLoaderConfig config);

}
