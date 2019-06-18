package com.latticeengines.datacloud.match.actors;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.actors.visitor.impl.AccountMatchPlannerMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.CachedDunsGuideValidateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DnBCacheLookupServiceImpl;
import com.latticeengines.datacloud.match.actors.visitor.impl.DnBLookupServiceImpl;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsDomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsGuideBookLookupServiceImpl;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsGuideValidateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DynamoDBLookupServiceImpl;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityAssociateServiceImpl;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityLookupServiceImpl;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntitySystemIdBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.MatchAnchorActor;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchKeyRecord;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput.EntityKeyMap;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

public class MatchActorSystemExcetionHandlingTestNG extends DataCloudMatchFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(MatchActorSystemExcetionHandlingTestNG.class);

    private static final Tenant TENANT = new Tenant(
            MatchActorSystemExcetionHandlingTestNG.class.getSimpleName() + UUID.randomUUID().toString());
    private static final Timeout TIMEOUT = new Timeout(new FiniteDuration(20, TimeUnit.SECONDS));

    private static final String DOMAIN_FLD = MatchKey.Domain.name();
    private static final String DUNS_FLD = MatchKey.DUNS.name();
    private static final String CAID_FLD = InterfaceName.CustomerAccountId.name();
    private static final String NAME_FLD = InterfaceName.Name.name();
    // use faked domain and duns to ensure it could reach name location
    // related actor
    private static final String DOMAIN_VAL = "testfakedomain.com";
    private static final String DUNS_VAL = "999999999";
    private static final String CAID_VAL = "1";
    private static final String NAME_VAL = "Google";

    private static final String FUZZY_MATCH_JUNCTION_ACTOR = "FuzzyMatchJunctionActor";

    @Value("${datacloud.match.default.decision.graph}")
    private String ldcMatchDG;

    @Value("${datacloud.match.default.decision.graph.account}")
    private String accountMatchDG;

    @Inject
    private MatchActorSystem actorSystem;

    @Inject
    private EntityMatchConfigurationService entityMatchConfigurationService;

    @BeforeClass(groups = "functional")
    public void init() {
        actorSystem.setBatchMode(false);
        entityMatchConfigurationService.setIsAllocateMode(true);
    }

    @Test(groups = "functional", dataProvider = "actorNames")
    private void test(String actorOrServiceToInjectFailure, boolean useDnBCache, String decisionGraph) {
        MatchInput input = prepareMatchInput(useDnBCache, decisionGraph);
        InternalOutputRecord record = prepareInternalOutputRecord();
        MatchTraveler traveler = prepareTraveler(input, record, actorOrServiceToInjectFailure);
        Future<Object> future = actorSystem.askAnchor(traveler, TIMEOUT);
        try {
            traveler = (MatchTraveler) Await.result(future, TIMEOUT.duration());
        } catch (Exception e) {
            Assert.fail("Fail to get future of match traveler", e);
        }
        Assert.assertTrue(CollectionUtils.isNotEmpty(traveler.getTravelErrors()));
        Assert.assertTrue(traveler.getTravelErrors().get(0).contains(actorOrServiceToInjectFailure));
        Assert.assertTrue(traveler.getTravelErrors().get(0).contains(ActorUtils.INJECTED_FAILURE_MSG));
    }

    // DO NOT enable parallelism:
    // Some actor batches requests to process. If a bunch of requests enter
    // actor system together, request with injected failure at certain actor
    // could fail other requests too
    @DataProvider(name = "actorNames", parallel = false)
    public Object[][] provideActorTestData() {
        // Schema: Actor/Service name, UseDnBCache, decisionGraph
        return new Object[][] { //
                // Actors
                { MatchAnchorActor.class.getSimpleName(), true, accountMatchDG }, //
                { EntitySystemIdBasedMicroEngineActor.class.getSimpleName(), true, accountMatchDG }, //
                { AccountMatchPlannerMicroEngineActor.class.getSimpleName(), true, accountMatchDG }, //
                { DunsDomainBasedMicroEngineActor.class.getSimpleName(), true, accountMatchDG }, //
                { FUZZY_MATCH_JUNCTION_ACTOR, true, accountMatchDG }, //
                { DunsGuideValidateMicroEngineActor.class.getSimpleName(), false, ldcMatchDG }, //
                { CachedDunsGuideValidateMicroEngineActor.class.getSimpleName(), true, ldcMatchDG }, //
                // Services
                { EntityLookupServiceImpl.class.getSimpleName(), true, accountMatchDG }, //
                { EntityAssociateServiceImpl.class.getSimpleName(), true, accountMatchDG }, //
                { DynamoDBLookupServiceImpl.class.getSimpleName(), true, accountMatchDG }, //
                { DnBCacheLookupServiceImpl.class.getSimpleName(), true, accountMatchDG }, //
                { DnBLookupServiceImpl.class.getSimpleName(), false, accountMatchDG }, //
                { DunsGuideBookLookupServiceImpl.class.getSimpleName(), false, ldcMatchDG }, //

        };
    };

    private MatchInput prepareMatchInput(boolean useDnBCache, String decisionGraph) {
        MatchInput matchInput = new MatchInput();
        matchInput.setLogLevelEnum(Level.DEBUG);
        matchInput.setDecisionGraph(decisionGraph);
        matchInput.setDataCloudVersion(currentDataCloudVersion);
        matchInput.setRootOperationUid(UUID.randomUUID().toString());
        matchInput.setUseDnBCache(useDnBCache);
        matchInput.setUseRemoteDnB(true);
        matchInput.setTenant(TENANT);
        matchInput.setAllocateId(true);
        matchInput.setFields(Arrays.asList(DOMAIN_FLD, DUNS_FLD, CAID_FLD, NAME_FLD));
        if (decisionGraph.equals(ldcMatchDG)) {
            matchInput.setKeyMap(prepareKeyMap());
            matchInput.setOperationalMode(OperationalMode.LDC_MATCH);
        } else {
            matchInput.setOperationalMode(OperationalMode.ENTITY_MATCH);
            matchInput.setEntityKeyMaps(prepareEntityKeyMap());
            matchInput.setTargetEntity(BusinessEntity.Account.name());
        }
        return matchInput;
    }

    private Map<MatchKey, List<String>> prepareKeyMap() {
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.Domain, Collections.singletonList(DOMAIN_FLD));
        keyMap.put(MatchKey.DUNS, Collections.singletonList(DUNS_FLD));
        keyMap.put(MatchKey.SystemId, Collections.singletonList(CAID_FLD));
        keyMap.put(MatchKey.Name, Collections.singletonList(NAME_FLD));
        return keyMap;
    }

    private Map<String, EntityKeyMap> prepareEntityKeyMap() {
        Map<String, EntityKeyMap> entityKeyMaps = new HashMap<>();
        EntityKeyMap entityKeyMap = new EntityKeyMap();
        entityKeyMap.setKeyMap(prepareKeyMap());
        entityKeyMaps.put(BusinessEntity.Account.name(), entityKeyMap);
        return entityKeyMaps;
    }

    private InternalOutputRecord prepareInternalOutputRecord() {
        InternalOutputRecord matchRecord = new InternalOutputRecord();
        matchRecord.setParsedDuns(DUNS_VAL);
        matchRecord.setParsedDomain(DOMAIN_VAL);
        NameLocation nl = new NameLocation();
        nl.setName(NAME_VAL);
        matchRecord.setParsedNameLocation(nl);
        matchRecord.setParsedTenant(EntityMatchUtils.newStandardizedTenant(TENANT));
        // same order as matchInput.fields
        Object[] rawData = new Object[] { DOMAIN_VAL, DUNS_VAL, CAID_VAL, NAME_VAL };
        matchRecord.setInput(Arrays.asList(rawData));
        // set position according to matchInput.fields
        Map<MatchKey, List<Integer>> keyPosMap = new HashMap<>();
        keyPosMap.put(MatchKey.Domain, Arrays.asList(0));
        keyPosMap.put(MatchKey.DUNS, Arrays.asList(1));
        keyPosMap.put(MatchKey.SystemId, Arrays.asList(2));
        keyPosMap.put(MatchKey.Name, Arrays.asList(3));
        Map<String, Map<MatchKey, List<Integer>>> entityKeyPositionMaps = new HashMap<>();
        entityKeyPositionMaps.put(BusinessEntity.Account.name(), keyPosMap);
        matchRecord.setEntityKeyPositionMap(entityKeyPositionMaps);
        return matchRecord;
    }

    private MatchTraveler prepareTraveler(MatchInput matchInput, InternalOutputRecord matchRecord,
            String actorNameToInjectFailure) {
        MatchTraveler matchTraveler = new MatchTraveler(matchInput.getRootOperationUid(), null);
        matchTraveler.setInputDataRecord(matchRecord.getInput());
        matchTraveler.setEntityKeyPositionMaps(matchRecord.getEntityKeyPositionMaps());
        EntityMatchKeyRecord entityMatchKeyRecord = new EntityMatchKeyRecord();
        entityMatchKeyRecord.setParsedTenant(matchRecord.getParsedTenant());
        matchTraveler.setEntityMatchKeyRecord(entityMatchKeyRecord);
        matchTraveler.setEntity(matchInput.getTargetEntity());
        matchTraveler.setMatchInput(matchInput);
        matchTraveler.setTravelTimeout(TIMEOUT);
        matchTraveler.setLogLevel(Level.DEBUG);
        matchTraveler.setActorOrServiceToInjectFailure(actorNameToInjectFailure);
        if (matchInput.getOperationalMode() == OperationalMode.LDC_MATCH) {
            MatchKeyTuple tuple = new MatchKeyTuple.Builder() //
                    .withDomain(matchRecord.getParsedDomain()) //
                    .withDuns(matchRecord.getParsedDuns()) //
                    .withName(matchRecord.getParsedNameLocation().getName()) //
                    .build();
            matchTraveler.setMatchKeyTuple(tuple);
        }
        return matchTraveler;
    }
}
