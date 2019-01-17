package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestEntityMatchService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

public class EntityMatchCorrectnessTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String TEST_TENANT_ID = EntityMatchCorrectnessTestNG.class.getSimpleName()
            + UUID.randomUUID().toString();
    private static final Tenant TEST_TENANT = new Tenant(TEST_TENANT_ID);
    private static final String ACCOUNT_DECISION_GRAPH = "PetitFour";

    /**
     * <pre>
     * FIXME change the field headers to use test helper classes Fields:
     * [ SFDC, MKTO, ELOQUA, Name, Domain, Country, State ]
     * </pre>
     */
    private static final String[] SYSTEM_ID_FIELDS = new String[] { "SFDC", "MKTO", "ELOQUA" };
    private static final MatchKey[] MATCH_KEY_FIELDS = new MatchKey[] { MatchKey.Name, MatchKey.Domain,
            MatchKey.Country, MatchKey.State };
    private static final String[] NON_SYSTEM_ID_FIELDS = Arrays.stream(MATCH_KEY_FIELDS).map(MatchKey::name)
            .toArray(String[]::new);
    private static final String[] FIELDS = ArrayUtils.addAll(SYSTEM_ID_FIELDS, NON_SYSTEM_ID_FIELDS);

    private static final EntityMatchEnvironment SOURCE_ENV = EntityMatchEnvironment.STAGING;
    private static final EntityMatchEnvironment DEST_ENV = EntityMatchEnvironment.SERVING;

    @Inject
    private RealTimeMatchService realTimeMatchService;

    @Inject
    private EntityRawSeedService entityRawSeedService;

    @Inject
    private EntityLookupEntryService entityLookupEntryService;

    @Inject
    private TestEntityMatchService testEntityMatchService;

    @Inject
    private EntityMatchConfigurationService entityMatchConfigurationService;

    @Test(groups = "functional", priority = 1)
    private void testAllocateAndLookup() {
        // prevent old data from affecting the test
        testEntityMatchService.bumpVersion(TEST_TENANT_ID);

        // test allocate mode
        List<Object> data = Arrays.asList("sfdc_1", "mkto_1", null, "GOOGLE", null, "USA", null);
        MatchOutput output = matchAccount(data, true);
        String entityId = verifyAndGetEntityId(output);

        // publish for testing lookup
        publishEntity(TEST_TENANT, BusinessEntity.Account.name());

        // test lookup, make sure we get the correct entity id with each match key
        Assert.assertEquals(lookupAccount("sfdc_1", null, null, null, null, null, null), entityId);
        Assert.assertEquals(lookupAccount(null, "mkto_1", null, null, null, null, null), entityId);
        Assert.assertEquals(lookupAccount(null, null, null, "GOOGLE", null, "USA", null), entityId);
    }

    @Test(groups = "functional", priority = 2)
    private void testPublicDomain() {
        // prevent old data from affecting the test
        testEntityMatchService.bumpVersion(TEST_TENANT_ID);

        // public domain without duns/name, and not in email format, treat as
        // normal domain
        List<Object> data = Arrays.asList(null, null, null, null, "gmail.com", "USA", null);
        MatchOutput output = matchAccount(data, true);
        String publicDomainEntityId = verifyAndGetEntityId(output);
        Assert.assertNotNull(publicDomainEntityId);

        // public domain without duns/name, but in email format, treat as public
        // domain
        data = Arrays.asList(null, null, null, null, "aaa@gmail.com", "USA", null);
        output = matchAccount(data, true);
        Assert.assertEquals(verifyAndGetEntityId(output), DataCloudConstants.ENTITY_ANONYMOUS_ID);

        // public domain with duns/name, , treat as public domain
        data = Arrays.asList(null, null, null, "public domain company name", "gmail.com", "USA", null);
        output = matchAccount(data, true);
        String entityId = verifyAndGetEntityId(output);
        Assert.assertNotNull(entityId);
        Assert.assertNotEquals(entityId, publicDomainEntityId);
    }

    // TODO add correctness test here

    private String lookupAccount(String sfdcId, String mktoId, String eloquaId, String name, String domain,
            String country, String state) {
        List<Object> data = Arrays.asList(sfdcId, mktoId, eloquaId, name, domain, country, state);
        MatchOutput output = matchAccount(data, false);
        return verifyAndGetEntityId(output);
    }

    /*
     * make sure that match output has exactly one row that only contains entityId
     * column and return the entityId
     */
    private String verifyAndGetEntityId(@NotNull MatchOutput output) {
        Assert.assertNotNull(output);
        Assert.assertNotNull(output.getResult());
        Assert.assertEquals(output.getResult().size(), 1);
        OutputRecord record = output.getResult().get(0);
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.getOutput());
        // check if output contains only entityId column
        Assert.assertEquals(output.getOutputFields(), Collections.singletonList(InterfaceName.EntityId.name()));
        Assert.assertEquals(record.getOutput().size(), 1);
        if (record.getOutput().get(0) != null) {
            Assert.assertTrue(record.getOutput().get(0) instanceof String);
        }
        return (String) record.getOutput().get(0);
    }

    private MatchOutput matchAccount(List<Object> data, boolean isAllocateMode) {
        entityMatchConfigurationService.setIsAllocateMode(isAllocateMode);
        String entity = BusinessEntity.Account.name();
        MatchInput input = prepareEntityMatchInput(TEST_TENANT, entity,
                Collections.singletonMap(entity, getEntityKeyMap(entity)), ACCOUNT_DECISION_GRAPH);
        input.setFields(Arrays.asList(FIELDS));
        input.setData(Collections.singletonList(data));
        return realTimeMatchService.match(input);
    }

    /*
     * helper to prepare basic MatchInput for entity match
     */
    private MatchInput prepareEntityMatchInput(@NotNull Tenant tenant, @NotNull String targetEntity,
            @NotNull Map<String, MatchInput.EntityKeyMap> entityKeyMaps, @NotNull String decisionGraph) {
        MatchInput input = new MatchInput();

        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        input.setTenant(tenant);
        input.setTargetEntity(targetEntity);
        // only support this predefined selection for now
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);
        input.setEntityKeyMaps(entityKeyMaps);
        input.setDataCloudVersion(currentDataCloudVersion);
        input.setDecisionGraph(decisionGraph);

        return input;
    }

    // FIXME move this to a common service (copied from CommitEntityMatch)
    private void publishEntity(Tenant tenant, String entity) {
        List<String> getSeedIds = new ArrayList<>();
        List<EntityRawSeed> scanSeeds = new ArrayList<>();
        do {
            Map<Integer, List<EntityRawSeed>> seeds = entityRawSeedService.scan(SOURCE_ENV, tenant, entity, getSeedIds,
                    1000);
            getSeedIds.clear();
            if (MapUtils.isNotEmpty(seeds)) {
                for (Map.Entry<Integer, List<EntityRawSeed>> entry : seeds.entrySet()) {
                    getSeedIds.add(entry.getValue().get(entry.getValue().size() - 1).getId());
                    scanSeeds.addAll(entry.getValue());
                }
                List<Pair<EntityLookupEntry, String>> pairs = new ArrayList<>();
                for (EntityRawSeed seed : scanSeeds) {
                    List<String> seedIds = entityLookupEntryService.get(SOURCE_ENV, tenant, seed.getLookupEntries());
                    for (int i = 0; i < seedIds.size(); i++) {
                        if (seedIds.get(i).equals(seed.getId())) {
                            pairs.add(Pair.of(seed.getLookupEntries().get(i), seedIds.get(i)));
                        }
                    }

                }
                // set TTL for test data
                entityRawSeedService.batchCreate(DEST_ENV, tenant, scanSeeds, true);
                entityLookupEntryService.set(DEST_ENV, tenant, pairs, true);
            }
            scanSeeds.clear();
        } while (CollectionUtils.isNotEmpty(getSeedIds));
    }

    private static MatchInput.EntityKeyMap getEntityKeyMap(@NotNull String entity) {
        MatchInput.EntityKeyMap map = new MatchInput.EntityKeyMap();
        map.setBusinessEntity(entity);
        map.setSystemIdPriority(Arrays.asList(SYSTEM_ID_FIELDS));
        Map<MatchKey, List<String>> fieldMap = Arrays.stream(MATCH_KEY_FIELDS).collect(
                Collectors.toMap(matchKey -> matchKey, matchKey -> Collections.singletonList(matchKey.name())));
        fieldMap.put(MatchKey.SystemId, Arrays.asList(SYSTEM_ID_FIELDS));
        map.setKeyMap(fieldMap);
        return map;
    }
}
