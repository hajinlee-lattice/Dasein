package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.cdl.scheduling.ActionStat;
import com.latticeengines.domain.exposed.pls.ActionType;

/**
 * Service to retrieve {@link ActionStat}
 */
public interface ActionStatService {

    /**
     * Retrieve all completed import actions that has no owner (not attached to any
     * job)
     *
     * @return list of stats, will not be {@literal null}
     */
    List<ActionStat> getNoOwnerCompletedIngestActionStats();

    /**
     * Retrieve action that has no owner and is not an ingestion action.
     *
     * @return list of stats, will not be {@literal null}
     */
    List<ActionStat> getNoOwnerNonIngestActionStats();

    /**
     * Retrieve all actions that has any one of target types and no owner (not
     * attached to any job)
     *
     * @param types
     *            target types
     * @return list of stats, will not be {@literal null}
     */
    List<ActionStat> getNoOwnerActionStatsByTypes(Set<ActionType> types);
}
