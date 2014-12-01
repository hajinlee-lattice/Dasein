package com.latticeengines.camille.config;

import com.latticeengines.camille.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class CustomerSpaceServiceConfigurationControllerImpl extends
        StandardConfigurationControllerImpl<CustomerSpaceServiceScope> {

    /**
     * Construct a configuration controller to access the customer space service
     * scope. Will attempt to bootstrap configuration directory when called.
     * 
     * @throws Exception
     *             Will throw if bootstrapping failed.
     */
    public CustomerSpaceServiceConfigurationControllerImpl(CustomerSpaceServiceScope scope) throws Exception {
        super(scope);
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);
    }
}
