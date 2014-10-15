package com.latticeengines.camille.config.cache;

import com.latticeengines.camille.CamilleCache;
import com.latticeengines.camille.DocumentSerializationException;
import com.latticeengines.camille.translators.PathTranslatorFactory;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class StandardConfigurationCacheControllerImpl<T extends ConfigurationScope> implements
        ConfigurationCacheControllerImpl<T> {

    protected final CamilleCache cache;

    public StandardConfigurationCacheControllerImpl(T scope, Path relativePath) throws Exception {
        cache = new CamilleCache(PathTranslatorFactory.getTranslator(scope).getAbsolutePath(relativePath));
        cache.start();
    }

    @Override
    public void rebuild() throws Exception {
        cache.rebuild();
    }

    @Override
    public Document get() throws DocumentSerializationException {
        return cache.get();
    }
}
