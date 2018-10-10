package com.latticeengines.domain.exposed.auth;

import io.swagger.annotations.ApiModel;

@ApiModel("Represents UserConfigProperty that is being used for dynamic mapping")
public enum UserConfigProperty {

    SSO_ENABLED(Boolean.class),
    FORCE_SSO_LOGIN(Boolean.class);

    private final Class<?> dataType;

    UserConfigProperty(Class<?> dataType) {
        this.dataType = dataType;
    }

    public Class<?> getDataType() {
        return dataType;
    }

}
