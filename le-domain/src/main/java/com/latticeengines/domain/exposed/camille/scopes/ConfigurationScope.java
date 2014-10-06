package com.latticeengines.domain.exposed.camille.scopes;

public abstract class ConfigurationScope {
    public enum Type {
        POD,
        CONTRACT,
        TENANT,
        SERVICE,
        CUSTOMER_SPACE,
        CUSTOMER_SPACE_SERVICE
    }
    
    public abstract Type getType();
}
