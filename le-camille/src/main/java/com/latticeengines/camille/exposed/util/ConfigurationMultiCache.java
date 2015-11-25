package com.latticeengines.camille.exposed.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.latticeengines.camille.exposed.config.cache.ConfigurationCache;
import com.latticeengines.camille.exposed.translators.PathTranslator;
import com.latticeengines.camille.exposed.translators.PathTranslatorFactory;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class ConfigurationMultiCache<T extends ConfigurationScope> {

    private ConfigurationMultiCache() throws Exception {
        caches = new HashMap<Path, ConfigurationCache<T>>();
    }

    public static <T extends ConfigurationScope> ConfigurationMultiCache<T> construct() {
        try {
            return new ConfigurationMultiCache<T>();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <D> D get(T scope, Path p, Class<D> clazz) {
        ConfigurationCache<T> cache = getCache(scope, p);
        Document doc = cache.get();
        if (StringUtils.isEmpty(doc.getData())) {
            throw new RuntimeException("Cannot deserialize emtry string to a type safe document.");
        }
        return DocumentUtils.toTypesafeDocument(doc, clazz);
    }

    public void rebuild(T scope, Path p) {
        ConfigurationCache<T> cache = getCache(scope, p);

        try {
            cache.rebuild();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ConfigurationCache<T> getCache(T scope, Path path) {
        Path absolute = getAbsolutePath(scope, path);
        if (!caches.containsKey(absolute)) {
            synchronized (caches) {
                if (!caches.containsKey(absolute)) {
                    caches.put(absolute, ConfigurationCache.construct(scope, path));
                }
            }
        }
        return caches.get(absolute);
    }

    private Path getAbsolutePath(T scope, Path path) {
        PathTranslator translator = PathTranslatorFactory.getTranslator(scope);
        try {
            return translator.getAbsolutePath(path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<Path, ConfigurationCache<T>> caches;
}