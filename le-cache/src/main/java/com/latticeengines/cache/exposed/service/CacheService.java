package com.latticeengines.cache.exposed.service;

import com.latticeengines.domain.exposed.cache.CacheNames;

public interface CacheService {

    void refreshKeysByPattern(String pattern, CacheNames... cacheNames);

}
