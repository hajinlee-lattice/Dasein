package com.latticeengines.camille.exposed.config.cache;

import com.latticeengines.camille.exposed.CamilleCache;
import com.latticeengines.camille.exposed.translators.PathTranslatorFactory;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class StandardConfigurationCacheImpl<T extends ConfigurationScope> implements ConfigurationCacheInterface<T> {

    protected final CamilleCache cache;

    public StandardConfigurationCacheImpl(T scope, Path relativePath) throws Exception {
        cache = new CamilleCache(PathTranslatorFactory.getTranslator(scope).getAbsolutePath(relativePath));
    }

    @Override
    public void rebuild() throws Exception {
        cache.rebuild();
    }

    @Override
    public Document get() {
        return cache.get();
    }
}
