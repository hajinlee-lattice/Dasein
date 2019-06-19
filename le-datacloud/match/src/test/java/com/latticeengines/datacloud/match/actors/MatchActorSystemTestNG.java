package com.latticeengines.datacloud.match.actors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchDecisionGraphService;
import com.latticeengines.datacloud.match.actors.visitor.impl.AccountMatchPlannerMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.CachedDunsValidateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.ContactMatchPlannerMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainCountryBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainCountryStateBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DomainCountryZipCodeBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsDomainBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.DunsValidateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityDomainCountryBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityDunsBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityEmailAIDBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityEmailBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityIdAssociateMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityIdResolveMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityNameCountryBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityNamePhoneAIDBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntityNamePhoneBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.EntitySystemIdBasedMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.LocationToCachedDunsMicroEngineActor;
import com.latticeengines.datacloud.match.actors.visitor.impl.LocationToDunsMicroEngineActor;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestEntityMatchService;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput.EntityKeyMap;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;


@Test
public class MatchActorSystemTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(MatchActorSystemTestNG.class);

    @Value("${datacloud.match.default.decision.graph}")
    private String ldcMatchDG;
    @Value("${datacloud.match.default.decision.graph.account}")
    private String accountMatchDG;
    @Value("${datacloud.match.default.decision.graph.contact}")
    private String contactMatchDG;

    private static final String DUNS = "079942718";
    private static final String DOMAIN = "abc.xyz";
    private static final String LATTICE_ID = "280002626626";
    private static final String CCID = "CCID1";
    private static final String EMAIL = "James@google.com";
    private static final String CNAME = "James Gosling";
    private static final String PHONE = "415-724-0000";

    private static final String DOMAIN_FLD = MatchKey.Domain.name();
    private static final String DUNS_FLD = MatchKey.DUNS.name();
    private static final String CCID_FLD = InterfaceName.CustomerContactId.name();
    private static final String EMAIL_FLD = "ContactEmail";
    private static final String CNAME_FLD = "ContactName";
    private static final String PHONE_FLD = MatchKey.PhoneNumber.name();

    private static final String FUZZY_MATCH_JUNCTION_ACTOR = "FuzzyMatchJunctionActor";
    private static final String ACCOUNT_MATCH_JUNCTION_ACTOR = "AccountMatchJunctionActor";
    private static final String[] LDC_TRAVEL_STOPS = { //
            DunsDomainBasedMicroEngineActor.class.getSimpleName(), //
    };
    private static final String[] LDC_TRAVEL_STOPS_FULL = { //
            DunsDomainBasedMicroEngineActor.class.getSimpleName(), //
            DunsBasedMicroEngineActor.class.getSimpleName(), //
            DomainCountryZipCodeBasedMicroEngineActor.class.getSimpleName(), //
            DomainCountryStateBasedMicroEngineActor.class.getSimpleName(), //
            DomainCountryBasedMicroEngineActor.class.getSimpleName(), //
            DomainBasedMicroEngineActor.class.getSimpleName(), //
            LocationToCachedDunsMicroEngineActor.class.getSimpleName(), //
            CachedDunsValidateMicroEngineActor.class.getSimpleName(), //
            LocationToDunsMicroEngineActor.class.getSimpleName(), //
            DunsValidateMicroEngineActor.class.getSimpleName(), //
    };
    private static final String[] ACCOUNT_TRAVEL_STOPS = {
            AccountMatchPlannerMicroEngineActor.class.getSimpleName(), //
            EntitySystemIdBasedMicroEngineActor.class.getSimpleName(), //
            FUZZY_MATCH_JUNCTION_ACTOR, //
            EntityDomainCountryBasedMicroEngineActor.class.getSimpleName(), //
            EntityNameCountryBasedMicroEngineActor.class.getSimpleName(), //
            EntityDunsBasedMicroEngineActor.class.getSimpleName(), //
            EntityIdAssociateMicroEngineActor.class.getSimpleName(), //
            EntityIdResolveMicroEngineActor.class.getSimpleName(), //
    };
    private static final String[] CONTACT_TRAVEL_STOPS = {
            ContactMatchPlannerMicroEngineActor.class.getSimpleName(), //
            EntitySystemIdBasedMicroEngineActor.class.getSimpleName(), //
            ACCOUNT_MATCH_JUNCTION_ACTOR, //
            EntityEmailAIDBasedMicroEngineActor.class.getSimpleName(), //
            EntityNamePhoneAIDBasedMicroEngineActor.class.getSimpleName(), //
            EntityEmailBasedMicroEngineActor.class.getSimpleName(), //
            EntityNamePhoneBasedMicroEngineActor.class.getSimpleName(), //
            EntityIdAssociateMicroEngineActor.class.getSimpleName(), //
    };

    private static final Map<String, String> DG_MAP = new HashMap<>();

    @Inject
    private FuzzyMatchService service;

    @Inject
    private MatchActorSystem actorSystem;

    @Inject
    private MatchDecisionGraphService matchDecisionGraphService;

    @Inject
    private EntityMatchConfigurationService entityMatchConfigurationService;

    @Inject
    private TestEntityMatchService testEntityMatchService;

    private static final Tenant TENANT = new Tenant(
            MatchActorSystemTestNG.class.getSimpleName() + UUID.randomUUID().toString());

    @BeforeClass(groups = "functional")
    public void init() {
        entityMatchConfigurationService.setIsAllocateMode(true);
        DG_MAP.put(BusinessEntity.LatticeAccount.name(), ldcMatchDG);
        DG_MAP.put(BusinessEntity.Account.name(), accountMatchDG);
        DG_MAP.put(BusinessEntity.Contact.name(), contactMatchDG);
    }

    // Realtime and batch mode cannot run at same time. Must be prioritized
    @Test(groups = "functional", dataProvider = "actorSystemTestData", priority = 1, enabled = true)
    public void testActorSystemRealtimeMode(int numRequests, String entity, String expectedID, String domain,
            String duns, String ccid, String email, String cname, String phone, boolean anonymousAcct,
            boolean existedAcct, boolean anonymousCtc) throws Exception {
        actorSystem.setBatchMode(false);
        testActorSystem(numRequests, entity, expectedID, domain, duns, ccid, email, cname, phone, anonymousAcct,
                existedAcct, anonymousCtc);
    }

    @Test(groups = "functional", dataProvider = "actorSystemTestData", priority = 2, enabled = true)
    public void testActorSystemBatchMode(int numRequests, String entity, String expectedID, String domain,
            String duns, String ccid, String email, String cname, String phone, boolean anonymousAcct,
            boolean existedAcct, boolean anonymousCtc) throws Exception {
        // If numRequests == 1, the test is designed to be no batch. Only test
        // single travel without retries
        if (numRequests == 1) {
            return;
        }
        actorSystem.setBatchMode(true);
        testActorSystem(numRequests * 10, entity, expectedID, domain, duns, ccid, email, cname, phone, anonymousAcct,
                existedAcct, anonymousCtc);
    }

    private void testActorSystem(int numRequests, String entity, String expectedID, String domain, String duns,
            String ccid, String email, String cname, String phone, boolean anonymousAcct, boolean existedAcct, boolean anonymousCtc)
            throws Exception {
        try {
            if (!existedAcct) {
                // Want to reuse Account universe built by previous test case
                testEntityMatchService.bumpVersion(TENANT.getId());
            }
            Integer maxRetries = null;
            try {
                DecisionGraph dg = matchDecisionGraphService.getDecisionGraph(DG_MAP.get(entity));
                maxRetries = dg.getRetries();
            } catch (ExecutionException e) {
                throw new RuntimeException("Fail to load decision graph " + DG_MAP.get(entity), e);
            }
            maxRetries = maxRetries == null ? 1 : maxRetries;

            MatchInput input = prepareMatchInput(entity);
            List<OutputRecord> matchRecords = prepareData(entity, numRequests, domain, duns, ccid, email, cname, phone);
            service.callMatch(matchRecords, input);

            boolean anonymous = (BusinessEntity.Account.name().equals(entity) && anonymousAcct)
                    || (BusinessEntity.Contact.name().equals(entity) && anonymousCtc);
            boolean hasError = false;
            String expectedEntityId = null;
            int numNewlyAllocatedAccount = 0;
            for (OutputRecord result : matchRecords) {
                int retries = getTravelRetriesFromTravelHistory(DG_MAP.get(entity), result.getMatchLogs());
                Assert.assertTrue(retries <= maxRetries);

                Queue<String> expectedTravelStops = parseTravelStops(entity, retries, anonymousAcct);
                Assert.assertNotNull(result);
                // Uncomment these lines for troubleshooting
                /*
                log.info(String.format("Retries = %d, Expected Stops = %s", retries,
                        String.join(",", expectedTravelStops)));
                log.info("MatchTravelerHistory");
                log.info(String.join("\n", result.getMatchLogs()));
                */

                // Verify travel history
                hasError = hasError || verifyTravelHistory(result.getMatchLogs(), expectedTravelStops);

                // Verify match result
                InternalOutputRecord record = (InternalOutputRecord) result;
                if (BusinessEntity.LatticeAccount.name().equals(entity)) {
                    hasError = hasError || verifyLDCMatchResult(record, expectedID);
                } else {
                    hasError = hasError || verifyEntityMatchResult(record, expectedEntityId, anonymous);
                    // Expect all the records match to same entity id as input
                    // is same
                    if (record.getEntityId() != null) {
                        expectedEntityId = record.getEntityId();
                    }
                    String accountEntityId = getNewAccountEntityId(record.getNewEntityIds());
                    if (StringUtils.isNotBlank(accountEntityId)) {
                        numNewlyAllocatedAccount++;
                    }
                }
            }
            if (BusinessEntity.LatticeAccount.name().equals(entity)) {
                Assert.assertEquals(numNewlyAllocatedAccount, 0, "LDC match should not allocate any new account.");
            } else if (!existedAcct && !anonymousAcct) {
                Assert.assertEquals(numNewlyAllocatedAccount, 1, "Should only have one newly allocated account.");
            }
            Assert.assertFalse(hasError, "There are errors, see logs above.");
        } finally {
            actorSystem.setBatchMode(false);
        }

    }

    // DO NOT enable parallel execution due to when existedAccount = false, that
    // test case has dependency on Account universe built by previous test case
    // Schema: #Row, Entity, ExpectedID, Domain, Duns, CCID, Email, ContactName,
    // Phone, anonymousAccount, existedAccount, anonymousContact
    @DataProvider(name = "actorSystemTestData", parallel = false)
    public Object[][] provideActorTestData() {
        return new Object[][] { //
                // LDC
                { 50, BusinessEntity.LatticeAccount.name(), LATTICE_ID, DOMAIN, DUNS, null, null, null, null, false,
                        false, false },
                // Account
                { 50, BusinessEntity.Account.name(), null, DOMAIN, DUNS, null, null, null, null, false, false, false }, //
                // Anonymous Account
                { 50, BusinessEntity.Account.name(), null, null, null, null, null, null, null, true, false, false }, //
                // Contact with Account creation (Single record, no retry in
                // both Contact & Account match)
                { 1, BusinessEntity.Contact.name(), null, DOMAIN, DUNS, CCID, EMAIL, CNAME, PHONE, false, false,
                        false }, //
                // To setup Account universe for Contact match without Account
                // creation
                { 50, BusinessEntity.Account.name(), null, DOMAIN, DUNS, null, null, null, null, false, false, false }, //
                // To test Contact retries without Account creation, otherwise
                // it could trigger Account match retries which makes travel
                // history very hard to verify
                // TODO: Will try to add test case of retrying both Contact &
                // Account later)
                { 50, BusinessEntity.Contact.name(), null, DOMAIN, DUNS, CCID, EMAIL, CNAME, PHONE, false, true,
                        false }, //
                // Contact with Anonymous Account
                { 50, BusinessEntity.Contact.name(), null, null, null, CCID, EMAIL, CNAME, PHONE, true, false, false }, //
                // Anonymous Contact
                { 50, BusinessEntity.Contact.name(), null, null, null, null, null, null, null, true, false, true }, //
        };
    }

    private int getTravelRetriesFromTravelHistory(String decisionGraph, List<String> travelLogs) {
        int retries = 1;
        Pattern pattern = Pattern.compile("Start traveling in decision graph " + decisionGraph + " for (\\d+) times");
        for (String log : travelLogs) {
            Matcher matcher = pattern.matcher(log);
            if (matcher.find()) {
                int round = Integer.valueOf(matcher.group(1));
                if (round > retries) {
                    retries = round;
                }
            }
        }
        return retries;
    }

    // TODO: Should have full travel stops in traveler instead of parsing
    // traveler log (Currently stop history only has)
    private boolean verifyTravelHistory(List<String> travelLogs, Queue<String> expectedTravelStops) {
        try {
            Pattern pattern = Pattern.compile("Arrived (.*?)\\.");
            travelLogs.forEach(log -> {
                Matcher matcher = pattern.matcher(log);
                if (matcher.find()) {
                    Assert.assertEquals(matcher.group(1), expectedTravelStops.peek());
                    expectedTravelStops.poll();
                }
            });
            Assert.assertTrue(expectedTravelStops.isEmpty());
        } catch (AssertionError e) {
            log.error("Exception in verifing travel history", e);
            return true;
        }
        return false;
    }

    private Queue<String> parseTravelStops(String entity, int retries, boolean anonymousAcct) {
        List<String> travelStops = new ArrayList<>();
        if (BusinessEntity.LatticeAccount.name().equals(entity)) {
            // LDC match decision graph does not retry
            travelStops.addAll(Arrays.asList(LDC_TRAVEL_STOPS));
        } else if (BusinessEntity.Account.name().equals(entity)) {
            // Only testing AllocateId mode for entity match which has retries.
            // Lookup mode in entity match does not have retries
            if (retries == 1) {
                travelStops.addAll(getAccountTravelStopsSingleRun(anonymousAcct));
            } else {
                travelStops.addAll(getAccountTravelStopsMultiRunsFor1stRun());
                for (int i = 2; i <= retries - 1; i++) {
                    travelStops.addAll(getAccountTravelStopsMultiRunsNoId());
                }
                travelStops.addAll(getAccountTravelStopsMultiRunsWithId());

            }
        } else if (BusinessEntity.Contact.name().equals(entity)) {
            travelStops.addAll(getContactTravelStopsSingleRun(anonymousAcct));
            for (int i = 2; i <= retries; i++) {
                travelStops.addAll(getContactTravelStopsRetriedRun());
            }
        } else {
            throw new IllegalArgumentException("Unhandled entity " + entity);
        }
        return new LinkedList<>(travelStops);

    }

    private String getNewAccountEntityId(Map<String, String> newEntityMap) {
        if (MapUtils.isEmpty(newEntityMap)) {
            return null;
        }
        return newEntityMap.get(BusinessEntity.Account.name());
    }

    private boolean verifyLDCMatchResult(InternalOutputRecord record, String expectedID) {
        try {
            Assert.assertEquals(record.getLatticeAccountId(), expectedID);
        } catch (AssertionError e) {
            log.error("Exception in verifing LDC match result", e);
            return true;
        }
        return false;
    }

    private boolean verifyEntityMatchResult(InternalOutputRecord record, String expectedEntityId, boolean anonymous) {
        try {
            if (anonymous) {
                Assert.assertEquals(record.getEntityId(), DataCloudConstants.ENTITY_ANONYMOUS_ID);
            } else if (expectedEntityId == null) {
                // expectedEntityId = null is not having expected entity id yet,
                // not expected to be null
                Assert.assertNotNull(record.getEntityId());
            } else {
                Assert.assertEquals(record.getEntityId(), expectedEntityId);
            }
        } catch (AssertionError e) {
            log.error("Exception in verifing entity match result", e);
            return true;
        }
        return false;
    }

    private MatchInput prepareMatchInput(String entity) {
        MatchInput matchInput = new MatchInput();
        matchInput.setLogLevelEnum(Level.DEBUG);
        matchInput.setDecisionGraph(DG_MAP.get(entity));
        matchInput.setDataCloudVersion(currentDataCloudVersion);
        matchInput.setRootOperationUid(UUID.randomUUID().toString());
        matchInput.setUseDnBCache(true);
        matchInput.setUseRemoteDnB(true);
        matchInput.setTenant(TENANT);
        matchInput.setAllocateId(true);
        matchInput.setEntityKeyMaps(prepareEntityKeyMap(entity));
        matchInput.setFields(Arrays.asList(DOMAIN_FLD, DUNS_FLD, CCID_FLD, EMAIL_FLD, CNAME_FLD, PHONE_FLD));
        matchInput.setTargetEntity(entity);
        // OperationalMode.CDL_LOOKUP case is not covered here
        if (matchInput.getTargetEntity().equals(BusinessEntity.LatticeAccount.name())) {
            matchInput.setOperationalMode(OperationalMode.LDC_MATCH);
        } else {
            matchInput.setOperationalMode(OperationalMode.ENTITY_MATCH);
        }
        return matchInput;
    }

    private Map<String, EntityKeyMap> prepareEntityKeyMap(String entity) {
        Map<String, EntityKeyMap> entityKeyMaps = new HashMap<>();

        // Both Account & Contact match needs Account key map
        EntityKeyMap entityKeyMap = new EntityKeyMap();
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.Domain, Collections.singletonList(DOMAIN_FLD));
        keyMap.put(MatchKey.DUNS, Collections.singletonList(DUNS_FLD));
        entityKeyMap.setKeyMap(keyMap);
        entityKeyMaps.put(BusinessEntity.Account.name(), entityKeyMap);

        if (BusinessEntity.Contact.name().equals(entity)) {
            entityKeyMap = new EntityKeyMap();
            keyMap = new HashMap<>();
            keyMap.put(MatchKey.SystemId, Collections.singletonList(CCID_FLD));
            keyMap.put(MatchKey.Email, Collections.singletonList(EMAIL_FLD));
            keyMap.put(MatchKey.Name, Collections.singletonList(CNAME_FLD));
            keyMap.put(MatchKey.PhoneNumber, Collections.singletonList(PHONE_FLD));
            entityKeyMap.setKeyMap(keyMap);
            entityKeyMaps.put(BusinessEntity.Contact.name(), entityKeyMap);
        }

        return entityKeyMaps;
    }

    private List<OutputRecord> prepareData(String entity, int numRecords, String domain, String duns, String ccid,
            String email, String cname, String phone) {
        List<OutputRecord> matchRecords = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            InternalOutputRecord matchRecord = new InternalOutputRecord();
            matchRecord.setParsedDuns(duns);
            matchRecord.setParsedDomain(domain);
            matchRecord.setParsedTenant(EntityMatchUtils.newStandardizedTenant(TENANT));
            // same order as matchInput.fields
            Object[] rawData = new Object[] { domain, duns, ccid, email, cname, phone };
            matchRecord.setInput(Arrays.asList(rawData));
            // set position according to matchInput.fields
            // Both Account & Contact match needs Account key map
            Map<MatchKey, List<Integer>> keyPosMap = new HashMap<>();
            keyPosMap.put(MatchKey.Domain, Arrays.asList(0));
            keyPosMap.put(MatchKey.DUNS, Arrays.asList(1));
            Map<String, Map<MatchKey, List<Integer>>> entityKeyPositionMaps = new HashMap<>();
            entityKeyPositionMaps.put(BusinessEntity.Account.name(), keyPosMap);
            if (BusinessEntity.Contact.name().equals(entity)) {
                keyPosMap = new HashMap<>();
                keyPosMap.put(MatchKey.SystemId, Arrays.asList(2));
                keyPosMap.put(MatchKey.Email, Arrays.asList(3));
                keyPosMap.put(MatchKey.Name, Arrays.asList(4));
                keyPosMap.put(MatchKey.PhoneNumber, Arrays.asList(5));
                entityKeyPositionMaps.put(BusinessEntity.Contact.name(), keyPosMap);
            }
            matchRecord.setEntityKeyPositionMap(entityKeyPositionMaps);
            matchRecords.add(matchRecord);
        }
        return matchRecords;
    }

    // Account: Only travel once
    private List<String> getAccountTravelStopsSingleRun(boolean anonymousAcct) {
        List<String> travelStops = new ArrayList<>(Arrays.asList(ACCOUNT_TRAVEL_STOPS));
        // Inject travel stops for ldc match decision graph
        int fuzzyMatchJunctionIdx = travelStops.indexOf(FUZZY_MATCH_JUNCTION_ACTOR);
        if (!anonymousAcct) {
            travelStops.addAll(fuzzyMatchJunctionIdx + 1, Arrays.asList(LDC_TRAVEL_STOPS));
        } else {
            travelStops.addAll(fuzzyMatchJunctionIdx + 1, Arrays.asList(LDC_TRAVEL_STOPS_FULL));
        }
        // Remove last EntityIdResolveMicroEngineActor because
        // EntityIdAssociateMicroEngineActor already gets a match
        travelStops.remove(travelStops.size() - 1);
        return travelStops;
    }

    // Account: If travel multiple times, for 1st run and no id associated
    private List<String> getAccountTravelStopsMultiRunsFor1stRun() {
        List<String> travelStops = new ArrayList<>(Arrays.asList(ACCOUNT_TRAVEL_STOPS));
        // Inject travel stops for ldc match decision graph
        int fuzzyMatchJunctionIdx = travelStops.indexOf(FUZZY_MATCH_JUNCTION_ACTOR);
        travelStops.addAll(fuzzyMatchJunctionIdx + 1, Arrays.asList(LDC_TRAVEL_STOPS));
        return travelStops;
    }

    // Account: If travel multiple times, for non-1st run and no id associated
    private List<String> getAccountTravelStopsMultiRunsNoId() {
        return new ArrayList<>(Arrays.asList(ACCOUNT_TRAVEL_STOPS));
    }

    // Account: If travel multiple times, for final run with id associated
    private List<String> getAccountTravelStopsMultiRunsWithId() {
        List<String> travelStops = new ArrayList<>(Arrays.asList(ACCOUNT_TRAVEL_STOPS));
        // Remove last EntityIdResolveMicroEngineActor because
        // EntityIdAssociateMicroEngineActor already gets a match
        travelStops.remove(travelStops.size() - 1);
        return travelStops;
    }

    // Contact: Only travel once or 1st run in multi-retries
    private List<String> getContactTravelStopsSingleRun(boolean anonymousAccount) {
        List<String> travelStops = new ArrayList<>(Arrays.asList(CONTACT_TRAVEL_STOPS));
        // Inject travel stops for account match decision graph
        int accountMatchJunctionIdx = travelStops.indexOf(ACCOUNT_MATCH_JUNCTION_ACTOR);
        //
        travelStops.addAll(accountMatchJunctionIdx + 1, getAccountTravelStopsSingleRun(anonymousAccount));
        return travelStops;
    }

    // Contact: If travel multiple times, for retried runs
    private List<String> getContactTravelStopsRetriedRun() {
        return new ArrayList<>(Arrays.asList(CONTACT_TRAVEL_STOPS));
    }
}
