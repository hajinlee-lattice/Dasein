package com.latticeengines.common.exposed.metric;

/**
 * Tag value constant class for metrics
 */
public final class MetricTags {

    protected MetricTags() {
        throw new UnsupportedOperationException();
    }

    public static final String TAG_TENANT = "Tenant";
    public static final String TAG_DYNAMO_TABLE = "Table";

    /*-
     * tags for match
     */
    public static class Match {
        public static final String TAG_ACTOR = "Actor";
        public static final String TAG_MATCHED = "Matched";
        public static final String TAG_PREDEFINED_SELECTION = "PredefinedSelection";
        public static final String TAG_REJECTED = "Rejected";
        public static final String TAG_SERVICE_NAME = "Service";
        public static final String TAG_MATCH_MODE = "MatchMode";
        public static final String TAG_DNB_CONFIDENCE_CODE = "DnBConfidenceCode";
        public static final String TAG_DNB_MATCH_GRADE = "DnBMatchGrade";
        public static final String TAG_DNB_MATCH_STRATEGY = "DnBMatchStrategy";
        public static final String TAG_DNB_HIT_WHITE_CACHE = "DnBHitWhiteCache";
        public static final String TAG_DNB_HIT_BLACK_CACHE = "DnBHitBlackCache";
        public static final String TAG_HAS_ERROR = "HasError";
        public static final String TAG_DATACLOUD_VERSION = "DataCloudVersion";
        public static final String TAG_OPERATIONAL_MODE = "OperationalMode";
    }

    /*-
     * tags for entity match
     */
    public static class EntityMatch {
        public static final String TAG_MATCH_ENV = "Environment";
        public static final String TAG_ALLOCATE_ID_MODE = "AllocateId";
        public static final String TAG_ENTITY = "Entity";
        public static final String TAG_IS_NEWLY_ALLOCATED = "NewlyAllocated";
    }

    /*-
     * tags for generic invocation meter
     */
    public static class Invocation {
        public static final String TAG_METHOD_NAME = "MethodName";
        public static final String TAG_HAS_ERROR = "HasError";
        public static final String TAG_CAN_IGNORE = "CanIgnore";
        // generic tags, add more as needed
        public static final String TAG_GENERIC = "Tag%02d";
    }
}
