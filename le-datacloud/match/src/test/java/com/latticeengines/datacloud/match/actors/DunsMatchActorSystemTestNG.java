package com.latticeengines.datacloud.match.actors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.service.NameLocationService;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchDecisionGraphService;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput.EntityKeyMap;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;


@Test
public class DunsMatchActorSystemTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DunsMatchActorSystemTestNG.class);

    @Value("${datacloud.match.default.decision.graph.prime}")
    private String primeMatchDG;

    @Inject
    private NameLocationService nameLocationService;

    @Inject
    private FuzzyMatchService service;

    @Inject
    private MatchActorSystem actorSystem;

    @Inject
    private MatchDecisionGraphService matchDecisionGraphService;

    private static final Tenant TENANT = new Tenant(
            DunsMatchActorSystemTestNG.class.getSimpleName() + UUID.randomUUID().toString());

    // Realtime and batch mode cannot run at same time. Must be prioritized
    @Test(groups = "functional", enabled = true)
    public void testActorSystemRealtimeMode() throws Exception {
        actorSystem.setBatchMode(false);
        testActorSystem();
    }

    @Test(groups = "functional", enabled = false)
    public void testActorSystemBatchMode() throws Exception {
        actorSystem.setBatchMode(true);
        testActorSystem();
    }

    private void testActorSystem() throws Exception {
        try {
            String entity = BusinessEntity.PrimeAccount.name();
            matchDecisionGraphService.getDecisionGraph(primeMatchDG);

            MatchInput input = prepareMatchInput(entity);
            List<OutputRecord> matchRecords = prepareData();
            service.callMatch(matchRecords, input);

            for (OutputRecord result : matchRecords) {
                result.getMatchLogs().forEach(log::info);
                // Verify match result
                InternalOutputRecord record = (InternalOutputRecord) result;
                List<DnBMatchCandidate> candidates = record.getCandidates();
                Assert.assertTrue(CollectionUtils.isNotEmpty(candidates));
                for (DnBMatchCandidate candidate: candidates) {
                    Assert.assertNotNull(candidate.getDuns());
                    NameLocation nameLocation = candidate.getNameLocation();
                    Assert.assertNotNull(nameLocation, JsonUtils.pprint(candidate));
                    Assert.assertNotNull(nameLocation.getName(), JsonUtils.pprint(candidate));
                    Assert.assertNotNull(nameLocation.getState(), JsonUtils.pprint(candidate));
                    Assert.assertNotNull(nameLocation.getCountryCode(), JsonUtils.pprint(candidate));
                }
            }
        } finally {
            actorSystem.setBatchMode(false);
        }

    }

    private MatchInput prepareMatchInput(String entity) {
        MatchInput matchInput = new MatchInput();
        matchInput.setLogLevelEnum(Level.DEBUG);
        matchInput.setDecisionGraph(primeMatchDG);
        matchInput.setDataCloudVersion(currentDataCloudVersion);
        matchInput.setRootOperationUid(UUID.randomUUID().toString());
        matchInput.setUseDnBCache(false);
        matchInput.setUseRemoteDnB(true);
        matchInput.setTenant(TENANT);
        matchInput.setAllocateId(false);
        matchInput.setEntityKeyMaps(prepareEntityKeyMap());
        matchInput.setFields(Arrays.asList("Duns", "CompanyName", "Sate", "Country"));
        matchInput.setTargetEntity(entity);
        matchInput.setOperationalMode(OperationalMode.MULTI_CANDIDATES);
        return matchInput;
    }

    private Map<String, EntityKeyMap> prepareEntityKeyMap() {
        Map<String, EntityKeyMap> entityKeyMaps = new HashMap<>();

        // Both Account & Contact match needs Account key map
        EntityKeyMap entityKeyMap = new EntityKeyMap();
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.DUNS, Collections.singletonList("DUNS"));
        keyMap.put(MatchKey.Name, Collections.singletonList("CompanyName"));
        keyMap.put(MatchKey.State, Collections.singletonList("State"));
        keyMap.put(MatchKey.Country, Collections.singletonList("Country"));
        entityKeyMap.setKeyMap(keyMap);
        entityKeyMaps.put(BusinessEntity.Account.name(), entityKeyMap);
        return entityKeyMaps;
    }

    private List<OutputRecord> prepareData() {
        List<OutputRecord> matchRecords = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            InternalOutputRecord matchRecord = new InternalOutputRecord();
            matchRecord.setParsedTenant(EntityMatchUtils.newStandardizedTenant(TENANT));
            // same order as matchInput.fields
            NameLocation nameLocation = new NameLocation();
            nameLocation.setName("GOOGLE LLC");
            nameLocation.setCountry("USA");
            nameLocation.setState("California");
            nameLocationService.normalize(nameLocation);
            matchRecord.setParsedNameLocation(nameLocation);
            Object[] rawData = new Object[] { null, "GOOGLE LLC", "California", "USA" };
            matchRecord.setInput(Arrays.asList(rawData));
            // set position according to matchInput.fields
            // Both Account & Contact match needs Account key map
            Map<MatchKey, List<Integer>> keyPosMap = new HashMap<>();
            keyPosMap.put(MatchKey.DUNS, Collections.singletonList(0));
            keyPosMap.put(MatchKey.Name, Collections.singletonList(1));
            keyPosMap.put(MatchKey.State, Collections.singletonList(2));
            keyPosMap.put(MatchKey.Country, Collections.singletonList(3));
            Map<String, Map<MatchKey, List<Integer>>> entityKeyPositionMaps = new HashMap<>();
            entityKeyPositionMaps.put(BusinessEntity.Account.name(), keyPosMap);
            matchRecord.setEntityKeyPositionMap(entityKeyPositionMaps);
            matchRecords.add(matchRecord);
        }
        return matchRecords;
    }
}
