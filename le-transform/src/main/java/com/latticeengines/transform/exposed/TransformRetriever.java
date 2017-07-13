package com.latticeengines.transform.exposed;

import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

@Service
public class TransformRetriever implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(TransformRetriever.class);
    private static final Marker fatal = MarkerFactory.getMarker("FATAL");

    @Value("${transform.transformer.cache.maxsize}")
    private int transformerCacheMaxSize;

    private LoadingCache<TransformId, RealTimeTransform> cache;

    public TransformRetriever() {
    }

    public RealTimeTransform getTransform(TransformId key) {
        try {
            return cache.getUnchecked(key);
        } catch (Exception e) {
            return null;
        }

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
                        String rtsClassName = getRTSClassFromPythonName(key.version, key.moduleName);
                        Constructor<RealTimeTransform> ctor = null;
                        try {
                            Class<RealTimeTransform> c = (Class<RealTimeTransform>) Class.forName(rtsClassName);
                            ctor = c.getConstructor(String.class);
                            log.info("Loaded java transform: " + key.moduleName + ". constructor name:"
                                    + ctor.getName());
                        } catch (Exception e) {
                            log.error(fatal, "Failed to load java transform " + rtsClassName, e);
                        }
                        return ctor.newInstance(new Object[] { key.modelPath });
                    }
                });
    }

}
