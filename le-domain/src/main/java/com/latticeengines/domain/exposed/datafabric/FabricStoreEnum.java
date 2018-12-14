package com.latticeengines.domain.exposed.datafabric;

import com.fasterxml.jackson.annotation.JsonValue;

public enum FabricStoreEnum {
    HDFS, REDIS, DYNAMO, S3;

    public static FabricStoreEnum typeOf(String name) {
        for (FabricStoreEnum enumName : FabricStoreEnum.values()) {
            if (enumName.name().equalsIgnoreCase(name)) {
                return enumName;
            }
        }
        return HDFS;
    }

    @JsonValue
    public String getName() {
        return name();
    }
}
