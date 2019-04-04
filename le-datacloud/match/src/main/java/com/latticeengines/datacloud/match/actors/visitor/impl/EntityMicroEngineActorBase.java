package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceMicroEngineTemplate;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceWrapperActorTemplate;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.service.EntityMatchMetricService;
import com.latticeengines.domain.exposed.actors.VisitingHistory;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityAssociationRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityAssociationResponse;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupResponse;
import com.latticeengines.domain.exposed.security.Tenant;

/**
 * Base micro engine class for entity match.
 *
 * @param <T> datasource actor used for lookup
 */
public abstract class EntityMicroEngineActorBase<T extends DataSourceWrapperActorTemplate>
        extends DataSourceMicroEngineTemplate<T> {
    private static final Logger log = LoggerFactory.getLogger(EntityMicroEngineActorBase.class);

    @Lazy
    @Inject
    private EntityMatchMetricService entityMatchMetricService;

    /**
     * Hook to decide whether this actor should process current request. If this method is invoked, all
     * fields required by entity match are guaranteed to exist.
     *
     * @param traveler current traveler instance
     * @return true if this actor should process this tuple, false otherwise.
     */
    protected abstract boolean shouldProcess(@NotNull MatchTraveler traveler);

    // For entity lookup actors. EntityIdAssociateActor and EntityIdResolveActor
    // both override this method
    @Override
    protected boolean skipIfRetravel(Traveler traveler) {
        if (traveler.getRetries() <= 1) {
            return false;
        }
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        return matchTraveler.hasCompleteLookupResults(getCurrentActorName());
    }

    @Override
    protected boolean accept(MatchTraveler traveler) {
        if (traveler.getMatchKeyTuple() == null || traveler.getMatchInput() == null) {
            log.error("Traveler missing MatchKeyTuple or MatchInput found in actor {}", self());
            return false;
        }
        if (StringUtils.isBlank(traveler.getEntity())) {
            log.error("Traveler with empty entity found in actor {}", self());
            return false;
        }
        MatchInput input = traveler.getMatchInput();
        if (input.getTenant() == null || StringUtils.isBlank(input.getTenant().getId())) {
            log.error("Traveler missing tenant found in actor {}", self());
            return false;
        }
        return shouldProcess(traveler);
    }

    @Override
    protected MatchKeyTuple prepareInputData(MatchKeyTuple input) {
        // not using legacy method
        return null;
    }

    /**
     * Prepare {@link EntityAssociationRequest} for entity match.
     *
     * @param traveler current match traveler instance
     * @return created request object, will not be {@literal null}
     */
    protected EntityAssociationRequest prepareAssociationRequest(@NotNull MatchTraveler traveler) {
        Tenant standardizedTenant = traveler.getEntityMatchKeyRecord().getParsedTenant();
        String entity = traveler.getEntity();
        List<Pair<MatchKeyTuple, String>> lookupResults = traveler.getEntityMatchLookupResults()
                .stream()
                .flatMap(pair -> {
                    MatchKeyTuple tuple = pair.getKey();
                    if (CollectionUtils.isNotEmpty(tuple.getSystemIds())) {
                        // flatten system id, one system id name/value pair per result
                        int size = tuple.getSystemIds().size();
                        return IntStream.range(0, size).mapToObj(idx -> {
                            Pair<String, String> systemIdPair = tuple.getSystemIds().get(idx);
                            if (StringUtils.isBlank(systemIdPair.getValue())) {
                                // user provide system column but blank value
                                return null;
                            }
                            MatchKeyTuple systemTuple = new MatchKeyTuple.Builder()
                                    .withSystemIds(Collections.singletonList(systemIdPair))
                                    .build();
                            // resulting entity id list should have the same size as systemIds
                            return Pair.of(systemTuple, pair.getValue().get(idx));
                        });
                    } else {
                        // non system id result, should only have one entity ID in the list
                        return Stream.of(Pair.of(pair.getKey(), pair.getValue().get(0)));
                    }
                }) //
                .filter(Objects::nonNull) //
                .collect(Collectors.toList());
        Map<String, String> extraAttributes = null;
        if (StringUtils.isNotBlank(traveler.getLatticeAccountId())) {
            extraAttributes = Collections.singletonMap(
                    DataCloudConstants.LATTICE_ACCOUNT_ID, traveler.getLatticeAccountId());
        }
        return new EntityAssociationRequest(standardizedTenant, entity, lookupResults, extraAttributes);
    }

    /**
     * Process the response that should contain an {@link EntityAssociationResponse} as result.
     *
     * @param response response object
     */
    protected void handleAssociationResponse(@NotNull Response response) {
        MatchTraveler traveler = (MatchTraveler) response.getTravelerContext();
        if (response.getResult() instanceof EntityAssociationResponse) {
            EntityAssociationResponse associationResponse = (EntityAssociationResponse) response.getResult();
            if (StringUtils.isNotBlank(associationResponse.getAssociatedEntityId())) {
                traveler.setMatched(true);
                traveler.setResult(associationResponse.getAssociatedEntityId());
                if (associationResponse.isNewlyAllocated()) {
                    // add newly allocated entity
                    traveler.addNewlyAllocatedEntityId(associationResponse.getAssociatedEntityId());
                }
                traveler.debug(String.format(
                        "Associate to entity successfully. Entity=%s, EntityId=%s, NewlyAllocated=%b, AssociationErrors=%s",
                        associationResponse.getEntity(), associationResponse.getAssociatedEntityId(), //
                        associationResponse.isNewlyAllocated(),
                        associationResponse.getAssociationErrors()));
            } else {
                // TODO log mode (bulk/realtime) and allocateId flag
                traveler.debug(String.format(
                        "Cannot associate to existing entity. Entity=%s, AssociationErrors=%s",
                        associationResponse.getEntity(), associationResponse.getAssociationErrors()));
            }

            if (CollectionUtils.isNotEmpty(associationResponse.getAssociationErrors())) {
                traveler.setEntityMatchErrors(associationResponse.getAssociationErrors());
            }
        } else {
            log.error("Got invalid entity association response in actor {}, should not have happened", self());
        }
    }

    /**
     * Prepare {@link EntityLookupRequest} for entity match.
     *
     * @param traveler current match traveler instance
     * @param tuple tuple that will be used for lookup
     * @return generated request object, will not be {@literal null}
     */
    protected EntityLookupRequest prepareLookupRequest(
            @NotNull MatchTraveler traveler, @NotNull MatchKeyTuple tuple) {
        Tenant standardizedTenant = traveler.getEntityMatchKeyRecord().getParsedTenant();
        String entity = traveler.getEntity();
        return new EntityLookupRequest(standardizedTenant, entity, tuple);
    }

    @Override
    protected void handleVisits(Traveler traveler, VisitingHistory history) {
        // for entity match actors, use micrometer for monitoring
        if (!(traveler instanceof MatchTraveler)) {
            return;
        }

        entityMatchMetricService.recordActorVisit((MatchTraveler) traveler, history);
    }

    /**
     * Process the response that should contain an {@link EntityLookupResponse} as result.
     *
     * @param response response object
     */
    protected void handleLookupResponse(@NotNull Response response) {
        MatchTraveler traveler = (MatchTraveler) response.getTravelerContext();
        if (response.getResult() instanceof EntityLookupResponse) {
            EntityLookupResponse lookupResponse = (EntityLookupResponse) response.getResult();
            if (foundEntity(lookupResponse)) {
                traveler.debug(String.format(
                        "Found EntityIds=%s for Entity=%s with MatchKeyTuple=%s",
                        lookupResponse.getEntityIds(), lookupResponse.getEntity(), lookupResponse.getTuple()));
            } else {
                traveler.debug(String.format(
                        "Cannot find any Entity=%s with MatchKeyTuple=%s",
                        lookupResponse.getEntity(), lookupResponse.getTuple()));
            }
            if (CollectionUtils.isEmpty(traveler.getEntityMatchLookupResults())) {
                // instantiate list
                traveler.setEntityMatchLookupResults(new ArrayList<>());
            }
            // add lookup result
            traveler.addLookupResult(getCurrentActorName(),
                    Pair.of(lookupResponse.getTuple(), lookupResponse.getEntityIds()));
        } else {
            log.error("Got invalid entity lookup response in actor {}, should not have happened", self());
        }
    }

    private boolean foundEntity(@NotNull EntityLookupResponse response) {
        if (CollectionUtils.isEmpty(response.getEntityIds())) {
            return false;
        }

        // see if we find any entity ID in the list
        Optional<String> entityId = response.getEntityIds().stream().filter(Objects::nonNull).findAny();
        return entityId.isPresent();
    }
}
