package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.List;
import java.util.Objects;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.template.ExecutorMicroEngineTemplate;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchGuideBook;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;

/**
 * Pick the entity found by the highest priority match key from a list of lookup
 * results. Only works in lookup mode.
 * ({@link EntityMatchConfigurationService#isAllocateMode(String)} = false)
 */
@Component("entityIdResolveMicroEngineActor")
@Scope("prototype")
public class EntityIdResolveMicroEngineActor extends ExecutorMicroEngineTemplate {

    private static final Logger log = LoggerFactory.getLogger(EntityIdResolveMicroEngineActor.class);

    @Resource(name = "matchActorSystem")
    private MatchActorSystem matchActorSystem;

    @Resource(name = "matchGuideBook")
    private MatchGuideBook guideBook;

    @Inject
    private EntityMatchConfigurationService entityMatchConfigurationService;

    @Value("${datacloud.match.entity.lookup.resolve.num.threads:4}")
    private int nExecutors;

    @Override
    protected int getExecutorNum() {
        return nExecutors;
    }

    @Override
    protected void execute(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;

        // For Lookup (Non-Allocate) Mode, capture entity's MatchLookupResults into entity map.
        matchTraveler.addEntityMatchLookupResults(matchTraveler.getEntity(), matchTraveler.getMatchLookupResults());

        // select the entityId found by the highest priority key
        String entityId = matchTraveler.getMatchLookupResults()
                .stream()
                .map(Pair::getValue)
                .filter(CollectionUtils::isNotEmpty)
                .flatMap(List::stream)
                .filter(Objects::nonNull) // null in the list means no entity found by that lookup entry
                .findFirst()
                .orElse(returnAnonymousWhenUnmatched(matchTraveler) ? DataCloudConstants.ENTITY_ANONYMOUS_ID : null);
        if (StringUtils.isNotBlank(entityId) && !DataCloudConstants.ENTITY_ANONYMOUS_ID.equals(entityId)) {
            matchTraveler.setMatched(true);
            matchTraveler.setResult(entityId);
            traveler.debug(String.format(
                    "Resolve lookup results to EntityId=%s for Entity=%s", entityId, matchTraveler.getEntity()));
        } else {
            if (StringUtils.isNotBlank(entityId)) {
                matchTraveler.setResult(entityId);
            }
            traveler.debug(String.format(
                    "No entity found in lookup results for Entity=%s", matchTraveler.getEntity()));
        }
    }

    @Override
    protected GuideBook getGuideBook() {
        return guideBook;
    }

    @Override
    protected ActorSystemTemplate getActorSystem() {
        return matchActorSystem;
    }

    @Override
    protected boolean accept(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        // in lookup mode and have lookup result
        return !entityMatchConfigurationService.isAllocateMode(matchTraveler.getEntity())
                && CollectionUtils.isNotEmpty(matchTraveler.getMatchLookupResults());
    }

    // Actually no retry for lookup mode. MatchAnchorActor decides it. Just
    // safeguard
    @Override
    protected boolean skipIfRetravel(Traveler traveler) {
        // Skip if it's retried travel
        return traveler.getRetries() > 1;
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof MatchTraveler;
    }

    private boolean returnAnonymousWhenUnmatched(MatchTraveler traveler) {
        if (traveler == null || traveler.getMatchInput() == null) {
            return false;
        }
        return traveler.getMatchInput().isReturnAnonymousWhenUnmatched();
    }
}
