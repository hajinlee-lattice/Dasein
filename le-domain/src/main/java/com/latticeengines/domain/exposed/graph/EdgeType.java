package com.latticeengines.domain.exposed.graph;

import java.util.HashMap;
import java.util.Map;

public final class EdgeType {

    protected EdgeType() {
        throw new UnsupportedOperationException();
    }

    public static final String TENANT = "Tenant";
    public static final String DEPENDS_ON = "DependsOn";
    public static final String DEPENDS_ON_FOR_TARGET = "DependsOnForTarget";
    public static final String DEPENDS_ON_FOR_TRAINING = "DependsOnForTraining";
    public static final String DEPENDS_ON_VIA_PA = "DependsOnViaPA";

    public static Map<String, String> typeToD3Type = new HashMap<>();

    static {
        typeToD3Type.put(DEPENDS_ON_FOR_TARGET, "dependsonfortarget");
        typeToD3Type.put(DEPENDS_ON_FOR_TRAINING, "dependsonfortraining");
        typeToD3Type.put(DEPENDS_ON_VIA_PA, "dependsonviapa");
        typeToD3Type.put(DEPENDS_ON, "dependson");
        typeToD3Type.put(TENANT, "tenant");
    }
}
