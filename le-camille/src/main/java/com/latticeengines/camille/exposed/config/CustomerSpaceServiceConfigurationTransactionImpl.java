package com.latticeengines.camille.exposed.config;

import com.latticeengines.camille.exposed.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;

public class CustomerSpaceServiceConfigurationTransactionImpl extends
        StandardConfigurationTransactionImpl<CustomerSpaceServiceScope> {

    /**
     * Construct a configuration transaction to access the customer space service
     * scope. Will attempt to bootstrap configuration directory when called.
     * 
     * @throws Exception
     *             Will throw if bootstrapping failed.
     */
    public CustomerSpaceServiceConfigurationTransactionImpl(CustomerSpaceServiceScope scope) throws Exception {
        super(scope);
        CustomerSpaceServiceBootstrapManager.bootstrap(scope);
    }
}
