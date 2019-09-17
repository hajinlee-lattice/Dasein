package com.latticeengines.matchapi.controller;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.InputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.matchapi.testframework.AdvancedMatchDeploymentTestNGBase;

public class SpecificVersionMatchDeploymentTestNG extends AdvancedMatchDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SpecificVersionMatchDeploymentTestNG.class);

    private static final String TEST_ENTITY = BusinessEntity.Account.name();
    private static final String[] FIELDS = { InterfaceName.CustomerAccountId.name() };
    private static final Object[][] TEST_DATA = { //
            { "acc1" }, //
            { "acc2" }, //
            { "acc3" }, //
    };

    private InputBuffer testDataBuffer;

    @BeforeClass(groups = "deployment")
    private void prepareData() {
        testDataBuffer = prepareStringData("shared", FIELDS, TEST_DATA);
    }

    @Test(groups = "deployment")
    private void testSpecificVersionMatch() throws Exception {
        int v1 = 1;
        int v2 = 2;

        // allocate data with v1
        MatchInput input = prepareMatchInput(true, v1);
        log.info("Allocate account with serving version v1 = {}", v1);
        Map<String, String> v1EntityIds = getEntityIds(runAndVerifyBulkMatch(input, getPodId()), true);
        log.info("Allocated entityIds for v1 = {}", v1EntityIds);
        Assert.assertNotNull(v1EntityIds);
        Assert.assertEquals(v1EntityIds.size(), TEST_DATA.length);
        // publish to v1 serving environment
        int nSeeds = publishEntity(TEST_ENTITY, v1);
        Assert.assertEquals(nSeeds, TEST_DATA.length);

        // clear staging and allocate with v2
        bumpStagingVersion();
        input = prepareMatchInput(true, v2);
        log.info("Allocate account with serving version v2 = {}", v2);
        Map<String, String> v2EntityIds = getEntityIds(runAndVerifyBulkMatch(input, getPodId()), true);
        log.info("Allocated entityIds for v2 = {}", v2EntityIds);
        Assert.assertNotNull(v2EntityIds);
        Assert.assertEquals(v2EntityIds.size(), TEST_DATA.length);
        Assert.assertNotEquals(v2EntityIds, v1EntityIds,
                "IDs allocated with v2 should be different than the ones with v1");
        // publish to v2
        nSeeds = publishEntity(TEST_ENTITY, v2);
        Assert.assertEquals(nSeeds, TEST_DATA.length);

        // lookup with v1
        log.info("Lookup account with serving version v1 = {}", v1);
        input = prepareMatchInput(false, v1);
        Map<String, String> v1LookupEntityIds = getEntityIds(runAndVerifyBulkMatch(input, getPodId()), true);
        Assert.assertEquals(v1LookupEntityIds, v1EntityIds);

        // lookup with v2, should match the IDs allocated by v2
        input = prepareMatchInput(false, v2);
        log.info("Lookup account with serving version v2 = {}", v2);
        Map<String, String> v2LookupEntityIds = getEntityIds(runAndVerifyBulkMatch(input, getPodId()), true);
        Assert.assertEquals(v2LookupEntityIds, v2EntityIds);
    }

    /*
     * CustomerAccountId -> EntityId
     */
    private Map<String, String> getEntityIds(MatchCommand result, boolean checkEntityId) throws Exception {
        List<GenericRecord> records = getRecords(result.getResultLocation(), true, checkEntityId);
        return getEntityIdMap(InterfaceName.CustomerAccountId.name(), records);
    }

    private MatchInput prepareMatchInput(boolean allocateId, Integer servingVersion) {
        MatchInput input = new MatchInput();
        input.setTenant(testTenant);
        input.setDataCloudVersion(currentDataCloudVersion);
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);
        input.setFields(Arrays.asList(FIELDS));
        input.setSkipKeyResolution(true);
        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        input.setTargetEntity(TEST_ENTITY);
        input.setAllocateId(allocateId);
        input.setOutputNewEntities(true);
        input.setEntityKeyMaps(getKeyMap());
        input.setServingVersion(servingVersion);
        input.setInputBuffer(testDataBuffer);
        input.setUseDnBCache(true);
        input.setUseRemoteDnB(true);
        return input;
    }

    private Map<String, MatchInput.EntityKeyMap> getKeyMap() {
        MatchInput.EntityKeyMap keyMap = new MatchInput.EntityKeyMap();
        keyMap.addMatchKey(MatchKey.SystemId, InterfaceName.CustomerAccountId.name());
        return Collections.singletonMap(TEST_ENTITY, keyMap);
    }
}
