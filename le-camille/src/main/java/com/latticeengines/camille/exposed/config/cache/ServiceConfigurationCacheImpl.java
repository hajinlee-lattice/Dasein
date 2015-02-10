package com.latticeengines.camille.exposed.config.cache;

import com.latticeengines.camille.exposed.CamilleCache;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceBootstrapManager;
import com.latticeengines.camille.exposed.translators.PathTranslatorFactory;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;
import com.latticeengines.domain.exposed.camille.scopes.ServiceScope;

public class ServiceConfigurationCacheImpl implements ConfigurationCacheInterface<CustomerSpaceServiceScope> {
    private CamilleCache cache;

    /**
     * Construct a configuration cache that accesses the service scope. Will
     * attempt to bootstrap configuration directory when called.
     * 
     * @throws Exception
     *             Will throw if bootstrapping failed.
     */
    public ServiceConfigurationCacheImpl(ServiceScope scope, Path relativePath) throws Exception {
        ServiceBootstrapManager.bootstrap(scope);
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
