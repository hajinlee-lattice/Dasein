package com.latticeengines.domain.exposed.graph;

public final class GraphConstants {

    protected GraphConstants() {
        throw new UnsupportedOperationException();
    }
    public static final String LABEL_SEP = "::";
    public static final String TENANT = "TENANT";
    public static final String SEGMENT = "Segment";
    public static final String RATING_ENGINE = "RatingEngine";
    public static final String PLAY = "Play";
    public static final String RATING_ENGINE_ATTR = "RatingEngineAttribute";
    public static final String CASCADE_ON_DELETE = "CASCADE_ON_DELETE";
    public static final String JUMP_DURING_DEP_CHECK = "JUMP_DURING_DEP_CHECK";

    public static final String NAME_PROP_KEY = "name";
    public static final String TENANT_ID_PROP_KEY = "tenantId";
    public static final String OBJECT_ID_KEY = "oId";
    public static final String OBJECT_TYPE_KEY = "oType";
    public static final String EDGE_TYPE_KEY = "eType";
    public static final String FROM_OID_KEY = "frOId";
    public static final String FROM_OTYPE_KEY = "frOType";
    public static final String TO_OID_KEY = "tOId";
    public static final String TO_OTYPE_KEY = "tOType";
    public static final String BEHAVIOR_ON_DEP_CHECK_TRAVERSAL_KEY = "DEP_CHECK_TRAVERSAL";
    public static final String BEHAVIOR_ON_DELETE_OF_IN_VERTEX_KEY = "DELETE_OF_IN_VERTEX";
    public static final String NS_KEY = "nsKey";
}
