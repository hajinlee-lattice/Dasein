package com.latticeengines.common.exposed.util;

public class ResourceUtils {

    public static final int profilingRatio = 10;

    public static final int modelingRatio = 10;

    public static long getProfilingMemory(long dataSize) {
        return dataSize * profilingRatio;
    }

    public static long getModelingMemory(long dataSize) {
        return dataSize * modelingRatio;
    }
}
