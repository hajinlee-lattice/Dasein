package com.latticeengines.camille.exposed.config;

import com.latticeengines.camille.exposed.config.bootstrap.ServiceBootstrapManager;
import com.latticeengines.domain.exposed.camille.scopes.ServiceScope;

public class ServiceConfigurationTransactionImpl extends StandardConfigurationTransactionImpl<ServiceScope> {

    /**
     * Construct a configuration transaction to access the service scope. Will
     * attempt to bootstrap configuration directory when called.
     * 
     * @throws Exception
     *             Will throw if bootstrapping failed.
     */
    public ServiceConfigurationTransactionImpl(ServiceScope scope) throws Exception {
        super(scope);
        ServiceBootstrapManager.bootstrap(scope);
    }
}
