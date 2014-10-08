package com.latticeengines.camille.config.cache;

import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class ConfigurationCacheController<T extends ConfigurationScope> implements ConfigurationCacheControllerImpl<T> {

    private ConfigurationCacheControllerImpl<T> impl;

    public ConfigurationCacheController(T scope) {

    }

    @Override
    public void doSomething() {
        // TODO Auto-generated method stub

    }

}
