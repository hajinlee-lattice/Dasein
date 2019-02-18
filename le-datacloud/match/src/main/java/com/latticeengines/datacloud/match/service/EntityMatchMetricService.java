package com.latticeengines.datacloud.match.service;

import org.springframework.retry.RetryContext;

import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.metric.FuzzyMatchHistory;
import com.latticeengines.domain.exposed.actors.VisitingHistory;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;

/**
 * Service for recording entity match related metrics
 */
public interface EntityMatchMetricService {

    /**
     * Record metrics for dynamo throttling event (read/write capacity exceeded)
     *
     * @param env
     *            current entity match env
     * @param tableName
     *            dynamo table name
     */
    void recordDynamoThrottling(EntityMatchEnvironment env, String tableName);

    /**
     * Record metrics for dynamo call.
     *
     * @param env
     *            current entity match environment
     * @param tableName
     *            dynamo table name
     * @param context
     *            retry context instance, containing retry count and other info
     * @param isThrottled
     *            whether is dynamo call failed due to throttling
     */
    void recordDynamoCall(EntityMatchEnvironment env, String tableName, RetryContext context, boolean isThrottled);

    /**
     * Record metrics a single visit on an actor for entity match. Noop if any of
     * the input is invalid or not from entity match.
     *
     * @param traveler
     *            current traveler instance
     * @param history
     *            target visit history
     */
    void recordActorVisit(MatchTraveler traveler, VisitingHistory history);

    /**
     * Record metrics for the entire match history of one single row. Noop if any of
     * the input is invalid or not from entity match.
     *
     * @param history
     *            entire entity match history
     */
    void recordMatchHistory(FuzzyMatchHistory history);
}
