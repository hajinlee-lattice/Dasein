package com.latticeengines.domain.exposed.camille.scopes;

public abstract class ConfigurationScope {
    public abstract Type getType();

    public enum Type {
        POD, POD_DIVISION, CONTRACT, TENANT, SERVICE, CUSTOMER_SPACE, CUSTOMER_SPACE_SERVICE
    }
}
