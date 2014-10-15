package com.latticeengines.camille.properties;

import com.latticeengines.domain.exposed.camille.scopes.ConfigurationScope;

public interface PropertiesManagerImpl<T extends ConfigurationScope> {
    public String getStringProperty(String name) throws Exception;

    public void setStringProperty(String name, String value) throws Exception;

    public double getDoubleProperty(String name) throws Exception;

    public void setDoubleProperty(String name, double value) throws Exception;

    public int getIntProperty(String name) throws Exception;

    public void setIntProperty(String name, int value) throws Exception;

    public boolean getBooleanProperty(String name) throws Exception;
}
