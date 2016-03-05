package com.latticeengines.scoringapi.transform;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.common.exposed.jython.JythonEngine;

@Service
public class TransformRetriever {

    private static final Log log = LogFactory.getLog(TransformRetriever.class);

    @Value("${scoringapi.transformer.cache.maxsize}")
    private int transformerCacheMaxSize;

    private LoadingCache<String, JythonEngine> cache;

    public TransformRetriever() {
        log.info("Instantiating transformer cache with max size " + transformerCacheMaxSize);
        cache = CacheBuilder.newBuilder().maximumSize(transformerCacheMaxSize)
                .build(new CacheLoader<String, JythonEngine>() {
                    @Override
                    public JythonEngine load(String key) throws Exception {
                        log.info("Loading jython engine " + key);
                        return new JythonEngine(key);
                    }
                });
    }

    public JythonEngine getTransform(String modelPath) {
        return cache.getUnchecked(modelPath);
    }

}
