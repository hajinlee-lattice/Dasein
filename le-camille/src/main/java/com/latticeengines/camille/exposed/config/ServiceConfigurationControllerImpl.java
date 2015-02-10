package com.latticeengines.camille.exposed.config;

import com.latticeengines.camille.exposed.config.bootstrap.ServiceBootstrapManager;
import com.latticeengines.domain.exposed.camille.scopes.ServiceScope;

public class ServiceConfigurationControllerImpl extends StandardConfigurationControllerImpl<ServiceScope> {

    /**
     * Construct a configuration controller to access the service scope. Will
     * attempt to bootstrap configuration directory when called.
     * 
     * @throws Exception
     *             Will throw if bootstrapping failed.
     */
    public ServiceConfigurationControllerImpl(ServiceScope scope) throws Exception {
        super(scope);
        ServiceBootstrapManager.bootstrap(scope);
    }
}
