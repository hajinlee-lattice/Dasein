package com.latticeengines.domain.exposed.camille.scopes;

import java.util.HashMap;
import java.util.Map;

public class ServiceScope extends ConfigurationScope {
    private String serviceName;
    private Map<String, String> properties;

    public ServiceScope(String serviceName) {
        this(serviceName, new HashMap<String, String>());
    }

    public ServiceScope(String serviceName, Map<String, String> properties) {
        this.serviceName = serviceName;
        this.setProperties(properties);
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((serviceName == null) ? 0 : serviceName.hashCode());
        result = prime * result + getType().hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ServiceScope other = (ServiceScope) obj;
        if (serviceName == null) {
            if (other.serviceName != null)
                return false;
        } else if (!serviceName.equals(other.serviceName))
            return false;
        return true;
    }

    @Override
    public Type getType() {
        return Type.SERVICE;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
