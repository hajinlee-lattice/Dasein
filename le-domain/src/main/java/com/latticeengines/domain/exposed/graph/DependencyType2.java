package com.latticeengines.domain.exposed.graph;

import java.util.HashMap;
import java.util.Map;

@Deprecated
public final class DependencyType2 {

    protected DependencyType2() {
        throw new UnsupportedOperationException();
    }

    public static final String TYPE_TENANT = "Tenant";

    public static final String TYPE_DEPENDS_ON = "DependsOn";

    public static final String TYPE_DEPENDS_ON_PA = "DependsOnPA";

    public static final String OBJ_PREFIX_TENANT = "TEN_";

    public static final String OBJ_PREFIX_SEGMENT = "SEG_";

    public static final String OBJ_PREFIX_RATING_ENGINE = "REN_";

    public static final String OBJ_PREFIX_ATTR_RATING = "ATTR.RID_";

    public static Map<String, String> typeToD3Type = new HashMap<>();

    static {
        typeToD3Type.put(TYPE_DEPENDS_ON_PA, "dependsonpa");
        typeToD3Type.put(TYPE_DEPENDS_ON, "dependson");
        typeToD3Type.put(TYPE_TENANT, "tenant");
    }

}
