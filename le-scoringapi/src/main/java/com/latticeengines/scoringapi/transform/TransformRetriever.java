package com.latticeengines.scoringapi.transform;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.latticeengines.common.exposed.jython.JythonEvaluator;
import com.latticeengines.scoringapi.model.ModelRetriever;

@Service
public class TransformRetriever {
    public TransformRetriever() {
        // TODO Make the cache properties tunable.
        cache = CacheBuilder.newBuilder().build(new CacheLoader<String, JythonTransform>() {
            @Override
            public JythonTransform load(String key) throws Exception {
                log.info("Retrieving jython transform " + key);
                String path = "com/latticeengines/domain/exposed/transforms/python/" + key + ".py";
                return new JythonTransform(JythonEvaluator.fromResource(path));
            }
        });
    }

    public JythonTransform getTransform(String name) {
        return cache.getUnchecked(name);
    }

    private static LoadingCache<String, JythonTransform> cache;

    private static final Log log = LogFactory.getLog(ModelRetriever.class);
}
