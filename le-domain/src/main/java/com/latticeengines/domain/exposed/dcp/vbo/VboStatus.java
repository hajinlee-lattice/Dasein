package com.latticeengines.domain.exposed.dcp.vbo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonFormat(shape = JsonFormat.Shape.OBJECT)
public enum VboStatus {
    SUCCESS("0", "Success", "Provisioning successful"),
    ALL_FAIL("10", "Failure", "Tenant provisioning and user creation failed"),
    USER_FAIL("11", "Failure", "Admin user creation failed, tenant provisioned"),
    PROVISION_FAIL("12", "Failure", "Tenant provisioning failed, user created"),
    OTHER("20", "Failure", "Unexpected error")
    ;

    @JsonProperty
    private final String code;

    @JsonProperty
    private final String message;

    @JsonProperty
    private final String description;

    VboStatus(String code, String message, String description) {
        this.code = code;
        this.message = message;
        this.description = description;
    }

    @JsonCreator
    public static VboStatus convert(@JsonProperty("code") String code) {
        for(VboStatus obj : VboStatus.values())
            if(obj.code.equals(code))
                return obj;

        return null;
    }
}
