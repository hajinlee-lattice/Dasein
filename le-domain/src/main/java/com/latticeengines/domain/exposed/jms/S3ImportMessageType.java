package com.latticeengines.domain.exposed.jms;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonValue;

public enum S3ImportMessageType {
    Atlas("Atlas"),
    DCP("DCP"),
    LISTSEGMENT("List Segment"),
    INBOUND_CONNECTION("Inbound Connection"),
    DATAOPERATION("Data Operation"),
    UNDEFINED("Undefined");

    private static Map<String, S3ImportMessageType> lookup = new HashMap<>();

    static {
        for (S3ImportMessageType type : S3ImportMessageType.values()) {
            lookup.put(type.getName(), type);
        }
    }

    private String name;

    S3ImportMessageType(String name) {
        this.name = name;
    }

    public static S3ImportMessageType fromName(String name) {
        return lookup.get(name);
    }

    @JsonValue
    public String getName() {
        return name;
    }
}
