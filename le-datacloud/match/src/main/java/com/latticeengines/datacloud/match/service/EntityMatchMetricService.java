package com.latticeengines.datacloud.match.service;

import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.metric.FuzzyMatchHistory;
import com.latticeengines.domain.exposed.actors.VisitingHistory;

/**
 * Service for recording entity match related metrics
 */
public interface EntityMatchMetricService {

    /**
     * Record metrics a single visit on an actor for entity match. Noop if any of the input is invalid or not from
     * entity match.
     *
     * @param traveler current traveler instance
     * @param history target visit history
     */
    void recordActorVisit(MatchTraveler traveler, VisitingHistory history);

    /**
     * Record metrics for the entire match history of one single row. Noop if any of the input is invalid or not from
     * entity match.
     *
     * @param history entire entity match history
     */
    void recordMatchHistory(FuzzyMatchHistory history);
}
