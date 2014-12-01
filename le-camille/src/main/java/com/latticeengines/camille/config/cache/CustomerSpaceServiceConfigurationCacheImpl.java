package com.latticeengines.camille.config.cache;

import com.latticeengines.camille.CamilleCache;
import com.latticeengines.camille.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.camille.translators.PathTranslatorFactory;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class CustomerSpaceServiceConfigurationCacheImpl implements ConfigurationCacheImpl<CustomerSpaceServiceScope> {
    private CamilleCache cache;
    
    /**
     * Construct a configuration cache that accesses the customer space service
     * scope. Will attempt to bootstrap configuration directory when called.
     * 
     * @throws Exception
     *             Will throw if bootstrapping failed.
     */
    public CustomerSpaceServiceConfigurationCacheImpl(CustomerSpaceServiceScope scope, Path relativePath)
            throws Exception {
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);
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
