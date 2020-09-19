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

import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchDecisionGraphService;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.util.EntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.security.Tenant;


@Test
public class ContactMatchActorSystemTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ContactMatchActorSystemTestNG.class);

    @Value("${datacloud.match.default.decision.graph.tps}")
    private String tpsDG;

    @Inject
    private FuzzyMatchService service;

    @Inject
    private MatchActorSystem actorSystem;

    @Inject
    private MatchDecisionGraphService matchDecisionGraphService;

    private static final Tenant TENANT = new Tenant(
            ContactMatchActorSystemTestNG.class.getSimpleName() + UUID.randomUUID().toString());

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
            String entity = ContactMasterConstants.MATCH_ENTITY_TPS;
            matchDecisionGraphService.getDecisionGraph(tpsDG);

            MatchInput input = prepareMatchInput(entity);
            List<OutputRecord> matchRecords = prepareData();
            service.callMatch(matchRecords, input);

            for (OutputRecord result : matchRecords) {
                result.getMatchLogs().forEach(log::info);
                // Verify match result
                InternalOutputRecord record = (InternalOutputRecord) result;
                List<String> fetchIds = record.getFetchIds();
                Assert.assertTrue(CollectionUtils.isNotEmpty(fetchIds));
                log.info("Matched to {} record ids.", fetchIds.size());
            }
        } finally {
            actorSystem.setBatchMode(false);
        }

    }

    private MatchInput prepareMatchInput(String entity) {
        MatchInput matchInput = new MatchInput();
        matchInput.setLogLevelEnum(Level.DEBUG);
        matchInput.setDecisionGraph(tpsDG);
        matchInput.setDataCloudVersion(currentDataCloudVersion);
        matchInput.setRootOperationUid(UUID.randomUUID().toString());
        matchInput.setTenant(TENANT);
        matchInput.setFields(Arrays.asList("ID", "SiteDuns"));
        matchInput.setKeyMap(prepareKeyMap());
        matchInput.setTargetEntity(entity);
        matchInput.setOperationalMode(OperationalMode.CONTACT_MATCH);
        return matchInput;
    }

    private Map<MatchKey, List<String>> prepareKeyMap() {
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.DUNS, Collections.singletonList("SiteDuns"));
        return keyMap;
    }

    private List<OutputRecord> prepareData() {
        List<OutputRecord> matchRecords = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            InternalOutputRecord matchRecord = new InternalOutputRecord();
            matchRecord.setParsedTenant(EntityMatchUtils.newStandardizedTenant(TENANT));
            // raw input
            Object[] rawData = new Object[] { 1, "028675958" };
            matchRecord.setInput(Arrays.asList(rawData));
            // parsed input
            matchRecord.setParsedDuns("028675958");
            matchRecords.add(matchRecord);
        }
        return matchRecords;
    }
}
