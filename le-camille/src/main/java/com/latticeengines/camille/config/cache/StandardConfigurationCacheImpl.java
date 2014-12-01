package com.latticeengines.camille.config.cache;

import com.latticeengines.camille.CamilleCache;
import com.latticeengines.camille.translators.PathTranslatorFactory;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class StandardConfigurationCacheImpl<T extends ConfigurationScope> implements ConfigurationCacheImpl<T> {

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
