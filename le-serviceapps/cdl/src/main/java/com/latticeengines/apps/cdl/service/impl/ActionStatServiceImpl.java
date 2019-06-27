package com.latticeengines.apps.cdl.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.query.Query;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.Sets;
import com.latticeengines.apps.cdl.service.ActionStatService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.cdl.scheduling.ActionStat;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

/**
 * Perliminary implementation that uses adhoc query. TODO either move the required stats to either {@link DataFeed}
 * or {@link DataFeedExecution} in the future and retire this class or move to other central location.
 */
@Lazy
@Component("actionStatService")
public class ActionStatServiceImpl implements ActionStatService {

    private static final Set<ActionType> INGESTION_TYPES = Sets.newHashSet( //
            ActionType.CDL_DATAFEED_IMPORT_WORKFLOW, //
            ActionType.CDL_OPERATION_WORKFLOW);
    private static final Set<String> RUNNING_STATUSES = Arrays.stream(JobStatus.values()) //
            .filter(status -> !status.isTerminated()) //
            .map(Enum::name) //
            .collect(Collectors.toSet());

    @Inject
    protected SessionFactory sessionFactory;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ActionStat> getNoOwnerCompletedIngestActionStats() {
        String queryStr = String.format(
                "SELECT NEW com.latticeengines.domain.exposed.cdl.scheduling.ActionStat(" + //
                        "a.tenant.pid, MIN(a.created), MAX(a.created)) " + //
                        "FROM %s as a JOIN %s as w " + //
                        "ON a.trackingPid = w.pid " + //
                "WHERE a.type in (:actionTypes) " + //
                "AND a.actionStatus = :actionStatus " + //
                "AND a.ownerId IS NULL " + //
                "AND w.status not in (:runningWorkflowStatuses) " + //
                "GROUP BY a.tenant.pid", Action.class.getSimpleName(), WorkflowJob.class.getSimpleName());
        Query<ActionStat> query = getBaseQuery(queryStr, INGESTION_TYPES);
        query.setParameter("runningWorkflowStatuses", RUNNING_STATUSES);
        return query.list();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ActionStat> getNoOwnerNonIngestActionStats() {
        String queryStr = String.format("SELECT NEW com.latticeengines.domain.exposed.cdl.scheduling.ActionStat("
                + "tenant.pid, MIN(created), MAX(created)) " + //
                "FROM %s " + //
                "WHERE type NOT IN (:actionTypes) AND type IS NOT NULL " + //
                "AND actionStatus = :actionStatus " + //
                "AND ownerId IS NULL " + //
                "GROUP BY tenant.pid", Action.class.getSimpleName());
        return getBaseQuery(queryStr, INGESTION_TYPES).list();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ActionStat> getNoOwnerActionStatsByTypes(Set<ActionType> types) {
        if (CollectionUtils.isEmpty(types)) {
            return Collections.emptyList();
        }

        String queryStr = String.format(
                "SELECT NEW com.latticeengines.domain.exposed.cdl.scheduling.ActionStat("
                        + "tenant.pid, MIN(created), MAX(created)) " + //
                "FROM %s " + //
                "WHERE type in (:actionTypes) " + //
                "AND actionStatus = :actionStatus " + //
                "AND ownerId IS NULL " + //
                "GROUP BY tenant.pid", Action.class.getSimpleName());
        return getBaseQuery(queryStr, types).list();
    }

    private Query<ActionStat> getBaseQuery(@NotNull String queryStr, @NotNull Set<ActionType> types) {
        Session session = sessionFactory.getCurrentSession();
        Query<ActionStat> query = session.createQuery(queryStr, ActionStat.class);
        query.setParameter("actionTypes", types);
        query.setParameter("actionStatus", ActionStatus.ACTIVE);
        return query;
    }
}
