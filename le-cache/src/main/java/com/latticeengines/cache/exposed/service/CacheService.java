package com.latticeengines.cache.exposed.service;

import com.latticeengines.domain.exposed.cache.CacheName;

public interface CacheService {

    void refreshKeysByPattern(String pattern, CacheName... cacheNames);

}
