package com.latticeengines.scoringapi.exposed.model.impl;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.Fields;

@Component
public class ModelFieldsCache {
    private static final Log log = LogFactory.getLog(ModelFieldsCache.class);

    @Value("${scoringapi.modeldetailsandfields.cache.maxsize}")
    private int modelDetailsAndFieldsCacheMaxSize;

    @Value("${scoringapi.modeldetailsandfields.cache.expiration.time}")
    private int modelDetailsAndFieldsCacheExpirationTime;

    private LoadingCache<AbstractMap.SimpleEntry<CustomerSpace, String>, Fields> modelFieldsCache;

    public void instantiateCache(ModelRetrieverImpl modelRetrieverImpl) {
        modelFieldsCache = CacheBuilder.newBuilder().maximumSize(modelDetailsAndFieldsCacheMaxSize) //
                .expireAfterAccess(modelDetailsAndFieldsCacheExpirationTime, TimeUnit.DAYS)
                .build(new CacheLoader<AbstractMap.SimpleEntry<CustomerSpace, String>, Fields>() {
                    @Override
                    public Fields load(AbstractMap.SimpleEntry<CustomerSpace, String> key) throws Exception {
                        if (log.isInfoEnabled()) {
                            log.info(String.format(
                                    "Load Fields tenant %s and model %s. "//
                                            + "Current cache size = %d",
                                    key.getKey(), key.getValue(), modelFieldsCache.asMap().size()));
                        }
                        Fields fields = modelRetrieverImpl.loadModelFieldsViaCache(key.getKey(), key.getValue());
                        if (log.isInfoEnabled()) {
                            log.info(String.format(
                                    "Load completed Fields for tenant %s and model %s. "//
                                            + "Current cache size = %d",
                                    key.getKey(), key.getValue(), modelFieldsCache.asMap().size()));
                        }
                        return fields;
                    };
                });
    }

    public LoadingCache<SimpleEntry<CustomerSpace, String>, Fields> getCache() {
        return modelFieldsCache;
    }

    @VisibleForTesting
    void setModelDetailsAndFieldsCacheMaxSize(int modelDetailsAndFieldsCacheMaxSize) {
        this.modelDetailsAndFieldsCacheMaxSize = modelDetailsAndFieldsCacheMaxSize;
    }

    @VisibleForTesting
    void setModelDetailsAndFieldsCacheExpirationTime(int modelDetailsAndFieldsCacheExpirationTime) {
        this.modelDetailsAndFieldsCacheExpirationTime = modelDetailsAndFieldsCacheExpirationTime;
    }
}
