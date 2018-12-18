package com.latticeengines.domain.exposed.aws;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public enum EC2InstanceType {

    m4_large("m4.large", 2, 8), //

    m5_xlarge("m5.xlarge", 4, 16), //
    m5_2xlarge("m5.2xlarge", 8, 32), //
    m5_4xlarge("m5.4xlarge", 16, 64), //

    r4_2xlarge("r4.2xlarge", 8, 61), //

    r5_2xlarge("r5.2xlarge", 8, 64), //
    r5d_xlarge("r5d.xlarge", 4, 32);

    private static Map<String, EC2InstanceType> nameMap;

    static {
        nameMap = new HashMap<>();
        for (EC2InstanceType instanceType : EC2InstanceType.values()) {
            nameMap.put(instanceType.getName(), instanceType);
        }
    }

    private final String name;
    private final int vCores;
    private final int memGb;

    EC2InstanceType(String name, int vCores, int memGb) {
        this.name = name;
        this.vCores = vCores;
        this.memGb = memGb;
    }

    public static EC2InstanceType fromName(String name) {
        if (StringUtils.isBlank(name)) {
            return null;
        }
        if (nameMap.containsKey(name)) {
            return nameMap.get(name);
        } else {
            throw new IllegalArgumentException("Cannot find a EC2InstanceType with name " + name);
        }
    }

    public String getName() {
        return name;
    }

    public int getvCores() {
        return vCores;
    }

    public int getMemGb() {
        return memGb;
    }
}
