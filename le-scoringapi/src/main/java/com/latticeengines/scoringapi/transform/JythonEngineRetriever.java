package com.latticeengines.scoringapi.transform;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.common.exposed.jython.JythonEngine;

@Component("jythonEngineRetriever")
public class JythonEngineRetriever implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(JythonEngineRetriever.class);

    @Value("${scoringapi.jythonengine.cache.maxsize}")
    private int jythonEngineCacheMaxSize;

    private LoadingCache<String, JythonEngine> cache;

    public JythonEngineRetriever() {
    }

    public JythonEngine getEngine(String key) {
        return cache.getUnchecked(key);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("Instantiating JythonEngine cache with max size " + jythonEngineCacheMaxSize);
        cache = CacheBuilder.newBuilder().maximumSize(jythonEngineCacheMaxSize) //
                .expireAfterAccess(30, TimeUnit.MINUTES) //
                .build(new CacheLoader<String, JythonEngine>() {
                    @Override
                    public JythonEngine load(String key) throws Exception {
                        log.info("Loading Jython engine from path " + key);
                        return new JythonEngine(key);
                    }
                });
    }

}
