package com.latticeengines.camille.properties;

import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public class PropertiesManager<T extends ConfigurationScope> implements PropertiesManagerImpl<T> {

    private PropertiesManagerImpl<T> impl;

    public PropertiesManager(T scope, Path path) throws Exception {
        impl = PropertiesManagerImplFactory.getImplementation(scope, path);
    }

    @Override
    public String getStringProperty(String name) throws Exception {
        return impl.getStringProperty(name);
    }

    @Override
    public void setStringProperty(String name, String value) throws Exception {
        impl.setStringProperty(name, value);
    }

    @Override
    public double getDoubleProperty(String name) throws Exception {
        return impl.getDoubleProperty(name);
    }

    @Override
    public void setDoubleProperty(String name, double value) throws Exception {
        impl.setDoubleProperty(name, value);
    }

    @Override
    public int getIntProperty(String name) throws Exception {
        return impl.getIntProperty(name);
    }

    @Override
    public void setIntProperty(String name, int value) throws Exception {
        impl.setIntProperty(name, value);
    }

    @Override
    public boolean getBooleanProperty(String name) throws Exception {
        return impl.getBooleanProperty(name);
    }
}
