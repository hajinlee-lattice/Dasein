package com.latticeengines.matchapi.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.InputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.matchapi.testframework.MatchapiDeploymentTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;

public class AccountMatchDeploymentTestNG extends MatchapiDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(AccountMatchDeploymentTestNG.class);

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Inject
    private TenantService tenantService;

    private static final String TENANT_ID = AccountMatchDeploymentTestNG.class.getSimpleName()
            + UUID.randomUUID().toString();
    private Tenant tenant = new Tenant(CustomerSpace.parse(TENANT_ID).toString());

    private static final String SFDC_ID = "SfdcId";
    private static final String TEST_ID = "TestId"; // To track test cases

    private static final String[] FIELDS = {
            TEST_ID, //
            MatchKey.Domain.name(), //
            MatchKey.DUNS.name(), //
            MatchKey.Name.name(), //
            MatchKey.Country.name(), //
            MatchKey.State.name(), //
            MatchKey.City.name(), //
            InterfaceName.AccountId.name(), //
            SFDC_ID, //
    };

    private static final String[] FIELDS_FETCHONLY = { //
            InterfaceName.EntityId.name(), //
    };

    private static final List<Class<?>> SCHEMA = new ArrayList<>(Collections.nCopies(FIELDS.length, String.class));

    private static final List<Class<?>> SCHEMA_FETCHONLY = new ArrayList<>(
            Collections.nCopies(FIELDS_FETCHONLY.length, String.class));

    /************************************************************************
     * Both DATA_ALL_KEYS & DATA_PARTIAL_KEYS should match to same EntityId
     ************************************************************************/
    // TODO: Change to duns = 079942718 when parent duns feature is
    // enabled
    // Domain, DUNS, Name, Country, State, City, AccountId, SfdcId
    private static final Object[][] DATA_ALL_KEYS = {
            { "C0_01", "google.com", "060902413", "google", "usa", "ca", "mountain view", "acc_id", "sfdc_id" }, //
    };

    // TODO: Change to duns = 079942718 when parent duns feature is
    // enabled
    // TestId, Domain, DUNS, Name, Country, State, City, SfdcId, MktoId
    private static final Object[][] DATA_PARTIAL_KEYS = {
            // case 1: duns only
            { "C1_01", null, "060902413", null, null, null, null, null, null }, //
            // missing leading 0 & with leading/trailing space
            { "C1_02", null, " 60902413 ", null, null, null, null, null, null }, //

            // case 2: name + country
            { "C2_01", null, null, "google", "usa", null, null, null, null }, //
            // with leading/trailing space
            { "C2_02", null, null, " google ", " usa ", null, null, null, null }, //

            // case 3: domain + country
            { "C3_01", "google.com ", null, null, "usa", null, null, null, null }, //
            // with leading/trailing space
            { "C3_02", " google.com ", null, null, " usa ", null, null, null, null }, //
            // non-standard domain + country
            { "C3_03", "www.google.com", null, null, "united states", null, null, null, null }, //

            // case 4: system id (currently don't have system id
            // standardization)
            { "C4_01", null, null, null, null, null, null, "acc_id", null }, //
            { "C4_02", null, null, null, null, null, null, null, "sfdc_id" }, //
            { "C4_03", null, null, null, null, null, null, "acc_id", "sfdc_id" }, //

            // case 5: any combinations
            // duns + name + country
            { "C5_01", null, "060902413", "google", "usa", null, null, null, null }, //
            // duns + domain + country
            { "C5_02", "google.com", "060902413", null, "usa", null, null, null, null }, //
            // duns + system id
            { "C5_03", null, "060902413", null, null, null, null, "acc_id", null }, //
            { "C5_04", null, "060902413", null, null, null, null, null, "sfdc_id" }, //
            { "C5_05", null, "060902413", null, null, null, null, "acc_id", "sfdc_id" }, //
            // name + domain + country
            { "C5_06", "google.com", null, "google", "usa", null, null, null, null }, //
            // name + country + system id
            { "C5_07", null, null, "google", "usa", null, null, "acc_id", null }, //
            { "C5_08", null, null, "google", "usa", null, null, null, "sfdc_id" }, //
            { "C5_09", null, null, "google", "usa", null, null, "acc_id", "sfdc_id" }, //
            // domain + country + system id
            { "C5_10", "google.com", null, null, "usa", null, null, "acc_id", null }, //
            { "C5_11", "google.com", null, null, "usa", null, null, null, "sfdc_id" }, //
            { "C5_12", "google.com", null, null, "usa", null, null, "acc_id", "sfdc_id" }, //
            // duns + name + domain + country
            { "C5_13", "google.com", "060902413", "google", "usa", null, null, null, null }, //
            // duns + name + country + system id
            { "C5_14", null, "060902413", "google", "usa", null, null, "acc_id", null }, //
            { "C5_15", null, "060902413", "google", "usa", null, null, null, "sfdc_id" }, //
            { "C5_16", null, "060902413", "google", "usa", null, null, "acc_id", "sfdc_id" }, //
            // duns + domain + country + system id
            { "C5_17", "google.com", "060902413", null, "usa", null, null, "acc_id", null }, //
            { "C5_18", "google.com", "060902413", null, "usa", null, null, null, "sfdc_id" }, //
            { "C5_19", "google.com", "060902413", null, "usa", null, null, "acc_id", "sfdc_id" }, //
            // name + domain + country + system id
            { "C5_20", "google.com", null, "google", "usa", null, null, "acc_id", null }, //
            { "C5_21", "google.com", null, "google", "usa", null, null, null, "sfdc_id" }, //
            { "C5_22", "google.com", null, "google", "usa", null, null, "acc_id", "sfdc_id" }, //
            // duns + name + domain + country + system id
            { "C5_23", "google.com", "060902413", "google", "usa", null, null, "acc_id", null }, //
            { "C5_24", "google.com", "060902413", "google", "usa", null, null, null, "sfdc_id" }, //
            { "C5_25", "google.com", "060902413", "google", "usa", null, null, "acc_id", "sfdc_id" }, //
    };

    // prepare in the run time because it needs EntityId got from non-fetch-only
    // mode test
    private Object[][] dataFetchOnly;


    private static final String CASE_ALL_KEYS = "ALL_KEYS";
    private static final String CASE_PARTIAL_KEYS = "PARTIAL_KEYS";

    private String resultEntityId = null;

    @BeforeClass(groups = "deployment")
    public void init() {
        HdfsPodContext.changeHdfsPodId(this.getClass().getSimpleName());
        cleanupAvroDir(hdfsPathBuilder.podDir().toString());

        tenant.setName(TENANT_ID);
        tenantService.registerTenant(tenant);
        // populate pid so that the tenant could be deleted in destroy()
        tenant = tenantService.findByTenantId(tenant.getId());
    }

    @AfterClass(groups = "deployment")
    public void destroy() {
        tenantService.discardTenant(tenant);
    }

    // One record with all match key populated
    @Test(groups = "deployment", priority = 1)
    public void testAllKeys() {
        MatchInput input = prepareBulkMatchInput(CASE_ALL_KEYS);
        runAndVerify(input);
    }

    // Records with partial match key (extracted from match key of #1)
    // populated, should all match to same EntityId in #1
    @Test(groups = "deployment", priority = 2)
    public void testPartialKeys() {
        MatchInput input = prepareBulkMatchInput(CASE_PARTIAL_KEYS);
        runAndVerify(input);
    }

    // Use EntityId got from #1 to test fetch-only mode
    @Test(groups = "deployment", priority = 3)
    public void testFetchOnly() {
        MatchInput input = prepareBulkMatchInputFetchOnly();
        runAndVerify(input);
    }

    private void runAndVerify(MatchInput input) {
        MatchCommand finalStatus = runAndVerifyBulkMatch(input, this.getClass().getSimpleName());
        if (input.isFetchOnly()) {
            validateBulkMatchResultFetchOnly(finalStatus.getResultLocation());
        } else {
            validateBulkMatchResult(finalStatus.getResultLocation());
        }
    }

    private MatchInput prepareBulkMatchInput(String scenario) {
        MatchInput input = new MatchInput();
        input.setTenant(tenant);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        input.setPredefinedSelection(Predefined.ID);
        input.setFields(Arrays.asList(FIELDS));
        input.setSkipKeyResolution(true);
        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        input.setTargetEntity(BusinessEntity.Account.name());
        input.setAllocateId(true);
        input.setEntityKeyMaps(prepareKeyMaps());
        input.setInputBuffer(prepareBulkData(scenario));
        input.setUseDnBCache(true);
        input.setUseRemoteDnB(true);
        return input;
    }

    private MatchInput prepareBulkMatchInputFetchOnly() {
        MatchInput input = new MatchInput();
        input.setTenant(tenant);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        input.setPredefinedSelection(Predefined.Seed);
        input.setFields(Arrays.asList(FIELDS_FETCHONLY));
        input.setSkipKeyResolution(true);
        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        input.setTargetEntity(BusinessEntity.Account.name());
        input.setFetchOnly(true);
        input.setEntityKeyMaps(prepareKeyMapsFetchOnly());
        input.setInputBuffer(prepareBulkDataFetchOnly());
        input.setUseDnBCache(true);
        input.setUseRemoteDnB(true);
        return input;
    }

    private Map<String, MatchInput.EntityKeyMap> prepareKeyMaps() {
        Map<String, MatchInput.EntityKeyMap> keyMaps = new HashMap<>();
        MatchInput.EntityKeyMap keyMap = new MatchInput.EntityKeyMap();
        keyMap.setBusinessEntity(BusinessEntity.Account.name());
        Map<MatchKey, List<String>> map = MatchKeyUtils.resolveKeyMap(Arrays.asList(FIELDS));
        map.put(MatchKey.SystemId, Arrays.asList(InterfaceName.AccountId.name(), SFDC_ID));
        keyMap.setKeyMap(map);
        keyMap.setSystemIdPriority(Arrays.asList(InterfaceName.AccountId.name(), SFDC_ID));
        keyMaps.put(BusinessEntity.Account.name(), keyMap);

        return keyMaps;
    }

    private Map<String, MatchInput.EntityKeyMap> prepareKeyMapsFetchOnly() {
        Map<String, MatchInput.EntityKeyMap> keyMaps = new HashMap<>();
        MatchInput.EntityKeyMap keyMap = new MatchInput.EntityKeyMap();
        keyMap.setBusinessEntity(BusinessEntity.Account.name());
        Map<MatchKey, List<String>> map = MatchKeyUtils.resolveKeyMap(Arrays.asList(FIELDS_FETCHONLY));
        map.put(MatchKey.EntityId, Arrays.asList(FIELDS_FETCHONLY));
        keyMap.setKeyMap(map);
        keyMaps.put(BusinessEntity.Account.name(), keyMap);
        return keyMaps;
    }

    private InputBuffer prepareBulkData(String scenario) {
        String avroDir = "/tmp/" + this.getClass().getSimpleName();
        cleanupAvroDir(avroDir);
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
        switch (scenario) {
        case CASE_ALL_KEYS:
            uploadAvroData(DATA_ALL_KEYS, Arrays.asList(FIELDS), SCHEMA, avroDir, CASE_ALL_KEYS + ".avro");
            break;
        case CASE_PARTIAL_KEYS:
            uploadAvroData(DATA_PARTIAL_KEYS, Arrays.asList(FIELDS), SCHEMA, avroDir, CASE_PARTIAL_KEYS + ".avro");
            break;
        default:
            throw new UnsupportedOperationException("Unknown test scenario " + scenario);
        }
        return inputBuffer;
    }

    private InputBuffer prepareBulkDataFetchOnly() {
        String avroDir = "/tmp/" + this.getClass().getSimpleName();
        cleanupAvroDir(avroDir);
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
        dataFetchOnly = new Object[][] {
                { resultEntityId }, //
                { "FakedEntityId" }, //
                { null }, //
        };
        uploadAvroData(dataFetchOnly, Arrays.asList(FIELDS_FETCHONLY), SCHEMA_FETCHONLY, avroDir, "FETCH_ONLY.avro");
        return inputBuffer;
    }

    // Designed test case that all of them should match to same EntityId
    private void validateBulkMatchResult(String path) {
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, path + "/*.avro");
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            if (resultEntityId == null) {
                resultEntityId = record.get(InterfaceName.EntityId.name()).toString();
            } else {
                Assert.assertEquals(record.get(InterfaceName.EntityId.name()).toString(), resultEntityId);
            }
        }
    }

    private void validateBulkMatchResultFetchOnly(String path) {
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, path + "/*.avro");
        int count = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            count++;
            log.info(record.toString());
            String entityId = record.get(InterfaceName.EntityId.name()) == null ? null
                    : record.get(InterfaceName.EntityId.name()).toString();
            if (resultEntityId.equals(entityId)) {
                Assert.assertNotNull(record.get(InterfaceName.LatticeAccountId.name()));
                Assert.assertEquals(record.get(InterfaceName.AccountId.name()).toString(), "acc_id");
            } else {
                Assert.assertNull(record.get(InterfaceName.LatticeAccountId.name()));
                Assert.assertNull(record.get(InterfaceName.AccountId.name()));
            }
        }
        Assert.assertEquals(count, dataFetchOnly.length);
    }
}
