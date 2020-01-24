package com.latticeengines.common.exposed.metric;

/**
 * constant class for all monitoring metric names
 */
public final class MetricNames {

    protected MetricNames() {
        throw new UnsupportedOperationException();
    }

    /*-
     * metrics for match
     */
    public static class Match {
        public static final String METRIC_MATCH_REQ = "match.req";
        public static final String METRIC_MATCH_DURATION = "match.duration";
        public static final String METRIC_TOTAL_MATCHED_ROWS = "match.matched.rows.total";
        public static final String METRIC_NUM_MATCH_INPUT_FIELDS = "match.num.input.fields";
        public static final String METRIC_NUM_MATCH_OUTPUT_FIELDS = "match.num.output.fields";
        public static final String METRIC_MATCH_BULK_SIZE = "match.bulk.size";
        public static final String METRIC_MATCH_RATE = "match.matched.rate";
        public static final String METRIC_ACTOR_VISIT = "match.actor.visit";
        public static final String METRIC_DNB_MATCH_HISTORY = "match.dnb.history";
        public static final String METRIC_PENDING_DATASOURCE_LOOKUP_REQ = "match.datasource.req.pending.count";
        public static final String METRIC_DATASOURCE_REQ_BATCH_SIZE = "match.datasource.req.batch.size.dist";
    }

    /*-
     * metrics for entity match specifically
     */
    public static class EntityMatch {
        public static final String METRIC_ACTOR_VISIT = "match.entity.actor.microengine";
        public static final String METRIC_HISTORY = "match.entity.history";
        public static final String METRIC_TRAVEL_ERROR = "match.entity.travel.error";
        public static final String METRIC_NUM_TRIES = "match.entity.num.tries";
        public static final String METRIC_DISTRIBUTION_RETRY = "match.entity.num.tries.dist.retry";
        public static final String METRIC_HAVE_RETRY_NUM_TRIES = "match.entity.num.tries.count.retry";
        public static final String METRIC_ASSOCIATION_CONFLICT_COUNT = "match.entity.associate.conflict.count";
        public static final String METRIC_ASSOCIATION_CONFLICT_DISTRIBUTION = "match.entity.associate.conflict.dist";
        public static final String METRIC_DYNAMO_THROTTLE = "match.entity.dynamo.throttling.count";
        public static final String METRIC_DYNAMO_CALL_ERROR_DIST = "match.entity.dynamo.call.error.dist";
        public static final String METRIC_DYNAMO_CALL_THROTTLE_DIST = "match.entity.dynamo.call.throttling.dist";
        public static final String METRIC_DYNAMO_CALL_RETRY_DIST = "match.entity.dynamo.call.retry.dist";
        public static final String METRIC_LOOKUP_CACHE = "match.entity.cache.lookup";
        public static final String METRIC_SEED_CACHE = "match.entity.cache.seed";
    }

    /*-
     * metrics for invocation meter
     */
    public static class Invocation {
        public static final String METRIC_INVOCATION_GLBOAL_HISTORY = "invocation.global.history";
        public static final String METRIC_INVOCATION_HISTORY = "invocation.history";
        public static final String METRIC_INVOCATION_ERROR = "invocation.error";
    }
}
