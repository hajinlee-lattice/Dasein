package com.latticeengines.scoringapi.transform;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.common.exposed.jython.JythonEngine;
import com.latticeengines.scoringapi.model.ModelRetriever;

@Service
public class TransformRetriever {
    public TransformRetriever() {
        // TODO Make the cache properties tunable.
        cache = CacheBuilder.newBuilder().build(new CacheLoader<String, JythonEngine>() {
            @Override
            public JythonEngine load(String key) throws Exception {
                log.info("Retrieving jython engine " + key);
                return new JythonEngine(key);
            }
        });
    }

    public JythonEngine getTransform(String modelPath) {
        return cache.getUnchecked(modelPath);
    }

    private static LoadingCache<String, JythonEngine> cache;

    private static final Log log = LogFactory.getLog(ModelRetriever.class);
}
