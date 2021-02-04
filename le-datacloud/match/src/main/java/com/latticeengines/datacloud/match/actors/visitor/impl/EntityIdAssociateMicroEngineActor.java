package com.latticeengines.datacloud.match.actors.visitor.impl;

import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.ACCT_EMAIL;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.ACCT_NAME_PHONE;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.EMAIL;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry.Type.NAME_PHONE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.EntityMatchInternalService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("entityIdAssociateMicroEngineActor")
@Scope("prototype")
public class EntityIdAssociateMicroEngineActor extends EntityMicroEngineActorBase<EntityAssociateActor> {

    @Inject
    private EntityMatchConfigurationService entityMatchConfigurationService;

    @Inject
    private EntityMatchInternalService entityMatchInternalService;

    @Value("${datacloud.match.entity.cfg.onlyAddDummyIfNotMapped:false}")
    private boolean onlyAddDummyIfNotMapped;

    @Override
    protected Class<EntityAssociateActor> getDataSourceActorClz() {
        return EntityAssociateActor.class;
    }

    @Override
    protected boolean shouldProcess(@NotNull MatchTraveler traveler) {
        // only accept if the system is in allocate mode
        return entityMatchConfigurationService.isAllocateMode(traveler.getEntity());
    }

    @Override
    protected void process(Response response) {
        handleAssociationResponse(response);
    }

    @Override
    protected Object prepareInputData(MatchTraveler traveler) {
        return prepareAssociationRequest(traveler);
    }

    @Override
    protected List<Pair<MatchKeyTuple, String>> postProcessLookupResults(@NotNull MatchTraveler traveler,
            @NotNull List<Pair<MatchKeyTuple, String>> results) {
        if (BusinessEntity.Contact.name().equals(traveler.getEntity()) && getAccountEntityId(traveler) != null) {
            // contact match & not matched to anonymous account
            List<Pair<MatchKeyTuple, String>> expandedResults = new ArrayList<>(results);
            expandedResults.addAll(
                    getFakeContactLookupResults(results, getAccountEntityId(traveler), traveler.getMatchInput()));
            return expandedResults;
        }
        return super.postProcessLookupResults(traveler, results);
    }

    // Always need to re-associate because the reason to retry is in previous
    // run, id is not successfully associated
    @Override
    protected boolean skipIfRetravel(Traveler traveler) {
        return false;
    }

    /**
     * Generate a list of fake lookup result that will be appended at the end of the
     * list for contact match. These fake results are used to setup extra lookup
     * entries.
     *
     * @return non-{@literal null} list of lookup results
     */
    protected List<Pair<MatchKeyTuple, String>> getFakeContactLookupResults(
            @NotNull List<Pair<MatchKeyTuple, String>> results, @NotNull String accountEntityId,
            @NotNull MatchInput input) {
        Map<EntityLookupEntry.Type, MatchKeyTuple> tupleMap = results.stream() //
                .filter(Objects::nonNull) //
                .map(Pair::getKey) //
                .filter(Objects::nonNull) //
                .map(tuple -> Pair.of(EntityLookupEntry.Type.identifyType(tuple), tuple)) //
                .filter(pair -> pair.getKey() == ACCT_EMAIL || pair.getKey() == EMAIL
                        || pair.getKey() == ACCT_NAME_PHONE || pair.getKey() == NAME_PHONE) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (v1, v2) -> v2));
        List<Pair<MatchKeyTuple, String>> fakeResults = new ArrayList<>();
        // fake lookup result to setup lookup entry for both AccountId+Email & Email
        // combinations
        if (tupleMap.containsKey(ACCT_EMAIL) && !tupleMap.containsKey(EMAIL)) {
            String email = tupleMap.get(ACCT_EMAIL).getEmail();
            MatchKeyTuple tuple = new MatchKeyTuple.Builder().withEmail(email).build();
            if (matchKeyNotMappedYet(tuple, input.getTenant(), input.getEntityMatchVersionMap())) {
                fakeResults.add(Pair.of(tuple, null));
            }
        } else if (tupleMap.containsKey(EMAIL) && !tupleMap.containsKey(ACCT_EMAIL)) {
            String email = tupleMap.get(EMAIL).getEmail();
            MatchKeyTuple tuple = accountEntityIdBuilder(accountEntityId).withEmail(email).build();
            if (matchKeyNotMappedYet(tuple, input.getTenant(), input.getEntityMatchVersionMap())) {
                fakeResults.add(Pair.of(tuple, null));
            }
        }

        // fake lookup result to setup lookup entry for both AccountId+Name+Phone &
        // Name+Phone combinations
        if (tupleMap.containsKey(ACCT_NAME_PHONE) && !tupleMap.containsKey(NAME_PHONE)) {
            MatchKeyTuple tuple = tupleMap.get(ACCT_NAME_PHONE);
            MatchKeyTuple accTuple = new MatchKeyTuple.Builder() //
                    .withName(tuple.getName()) //
                    .withPhoneNumber(tuple.getPhoneNumber()) //
                    .build();
            if (matchKeyNotMappedYet(accTuple, input.getTenant(), input.getEntityMatchVersionMap())) {
                fakeResults.add(Pair.of(accTuple, null));
            }
        } else if (tupleMap.containsKey(NAME_PHONE) && !tupleMap.containsKey(ACCT_NAME_PHONE)) {
            MatchKeyTuple tuple = tupleMap.get(NAME_PHONE);
            MatchKeyTuple accTuple = accountEntityIdBuilder(accountEntityId) //
                    .withName(tuple.getName()) //
                    .withPhoneNumber(tuple.getPhoneNumber()) //
                    .build();
            if (matchKeyNotMappedYet(accTuple, input.getTenant(), input.getEntityMatchVersionMap())) {
                fakeResults.add(Pair.of(accTuple, null));
            }
        }

        return fakeResults;
    }

    private boolean matchKeyNotMappedYet(@NotNull MatchKeyTuple tuple, @NotNull Tenant tenant,
            Map<EntityMatchEnvironment, Integer> versionMap) {
        if (!onlyAddDummyIfNotMapped) {
            // old behavior, always add dummy lookup entry
            return true;
        }

        // do an additional lookup and only add dummy entry if it's not mapped.
        // basically trading off read to save writes since write is the primary
        // bottleneck
        List<EntityLookupEntry> entries = EntityLookupEntryConverter.fromMatchKeyTuple(BusinessEntity.Contact.name(),
                tuple);
        List<String> ids = entityMatchInternalService.getIds(tenant, entries, versionMap);
        return ids.get(0) == null;
    }

    private MatchKeyTuple.Builder accountEntityIdBuilder(@NotNull String accountEntityId) {
        Pair<String, String> accountEntityIdPair = Pair.of(InterfaceName.AccountId.name(), accountEntityId);
        return new MatchKeyTuple.Builder().withSystemIds(ImmutableList.of(accountEntityIdPair));
    }

    private String getAccountEntityId(@NotNull MatchTraveler traveler) {
        if (MapUtils.isEmpty(traveler.getEntityIds())) {
            return null;
        }
        String accountEntityId = traveler.getEntityIds().get(BusinessEntity.Account.name());
        if (StringUtils.isBlank(accountEntityId) || DataCloudConstants.ENTITY_ANONYMOUS_ID.equals(accountEntityId)) {
            return null;
        }

        return accountEntityId;
    }
}
