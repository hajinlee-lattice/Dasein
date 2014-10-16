package com.latticeengines.camille.config.cache;

import com.latticeengines.camille.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class CustomerSpaceServiceConfigurationCacheImpl extends
        StandardConfigurationCacheImpl<CustomerSpaceServiceScope> {

    /**
     * Construct a configuration cache that accesses the customer space service
     * scope. Will attempt to bootstrap configuration directory when called.
     * 
     * @throws Exception
     *             Will throw if bootstrapping failed.
     */
    public CustomerSpaceServiceConfigurationCacheImpl(CustomerSpaceServiceScope scope, Path relativePath)
            throws Exception {
        super(scope, relativePath);
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);
    }
}
