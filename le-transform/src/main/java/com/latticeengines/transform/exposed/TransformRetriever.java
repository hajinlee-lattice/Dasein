package com.latticeengines.transform.exposed;

import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

@Service
public class TransformRetriever implements InitializingBean {

    private static final Log log = LogFactory.getLog(TransformRetriever.class);

    @Value("${transform.transformer.cache.maxsize}")
    private int transformerCacheMaxSize;

    private LoadingCache<TransformId, RealTimeTransform> cache;

    public TransformRetriever() {
    }

    public RealTimeTransform getTransform(TransformId key) {
        return cache.getUnchecked(key);
    }

    String getRTSClassFromPythonName(String version, String pythonModuleName) {
        if (version == null) {
            version = "v2_0_25";
        }

        String[] tokens = pythonModuleName.split("_");
        StringBuilder sb = new StringBuilder("com.latticeengines.transform." + version + ".functions.");

        for (String token : tokens) {
            sb.append(StringUtils.capitalize(token));
        }
        return sb.toString();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("Instantiating transformer cache with max size " + transformerCacheMaxSize);
        cache = CacheBuilder.newBuilder().maximumSize(transformerCacheMaxSize) //
                .expireAfterAccess(30, TimeUnit.MINUTES) //
                .build(new CacheLoader<TransformId, RealTimeTransform>() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public RealTimeTransform load(TransformId key) throws Exception {
                        log.info("Loading transform " + key.moduleName);

                        Class<RealTimeTransform> c = (Class<RealTimeTransform>) Class
                                .forName(getRTSClassFromPythonName(key.version, key.moduleName));
                        Constructor<RealTimeTransform> ctor = c.getConstructor(String.class);
                        return ctor.newInstance(new Object[] { key.modelPath });
                    }
                });
    }

}