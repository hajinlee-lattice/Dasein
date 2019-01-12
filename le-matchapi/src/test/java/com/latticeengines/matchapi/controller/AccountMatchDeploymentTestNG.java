package com.latticeengines.matchapi.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.InputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.matchapi.testframework.MatchapiDeploymentTestNGBase;

public class AccountMatchDeploymentTestNG extends MatchapiDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(AccountMatchDeploymentTestNG.class);

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Inject
    private EntityMatchVersionService entityMatchVersionService;

    private static final Tenant TENANT = new Tenant(DataCloudConstants.SERVICE_TENANT);

    private static final String SFDC_ID = "SfdcId";
    private static final String MKTO_ID = "MktoId";

    private static final String[] FIELDS = {
            MatchKey.Domain.name(), //
            MatchKey.DUNS.name(), //
            MatchKey.Name.name(), //
            MatchKey.Country.name(), //
            MatchKey.State.name(), //
            MatchKey.City.name(), //
            SFDC_ID, //
            MKTO_ID, //
    };

    private static final List<Class<?>> SCHEMA = new ArrayList<>(Collections.nCopies(FIELDS.length, String.class));

    // TODO: Change to duns = 079942718 when parent duns feature is
    // enabled
    // Domain, DUNS, Name, Country, State, City, SfdcId, MktoId
    private static final Object[][] DATA_ALL_KEYS = {
            { "google.com", "060902413", "google", "usa", "ca", "mountain view", "sfdc_id", "mkto_id" }, //
    };

    // TODO: Change to duns = 079942718 when parent duns feature is
    // enabled
    // Domain, DUNS, Name, Country, State, City, SfdcId, MktoId
    private static final Object[][] DATA_PARTIAL_KEYS = {
            // case 1: duns only
            { null, "060902413", null, null, null, null, null, null }, //
            // missing leading 0
            { null, "60902413", null, null, null, null, null, null }, //

            // case 2: name + location
            { null, null, "google", "usa", "ca", "mountain view", null, null }, //
            // with leading/trailing space
            { null, null, " google ", " usa ", " ca ", " mountain view ", null, null }, //

            // case 3: domain + country (not implemented yet)
            // { "google.com ", null, null, "usa", null, null, null, null }, //

            // TODO(@Stephen) uncomment following cases to troubleshoot dynamo
            // issue
            /*
            // case 4: domain + name/location
            { "google.com", null, "google", "usa", "ca", "mountain view", null, null }, //
            // non-standard domain + name/location
            { "www.google.com", null, " google ", "united states", "california", " mountain view ", null, null }, //

            // case 5: system id (currently don't have system id
            // standardization)
            { null, null, null, null, null, null, "sfdc_id", null }, //
            { null, null, null, null, null, null, null, "mkto_id" }, //
            { null, null, null, null, null, null, "sfdc_id", "mkto_id" }, //


            // case 6: domain + name/location + system id
            //{ "google.com", null, "google", "usa", "ca", "mountain view", "sfdc_id", null }, //
            //{ "google.com", null, "google", "usa", "ca", "mountain view", null, "mkto_id" }, //
            */
    };


    private static final String CASE_ALL_KEYS = "ALL_KEYS";
    private static final String CASE_PARTIAL_KEYS = "PARTIAL_KEYS";

    private String resultEntityId = null;

    @PostConstruct
    public void init() {
        entityMatchVersionService.bumpVersion(EntityMatchEnvironment.STAGING, TENANT);
    }

    @Test(groups = "deployment", priority = 1)
    public void testAllKeys() {
        MatchInput input = prepareBulkMatchInput(CASE_ALL_KEYS);
        runAndVerify(input);
    }

    @Test(groups = "deployment", priority = 2)
    public void testPartialKeys() {
        MatchInput input = prepareBulkMatchInput(CASE_PARTIAL_KEYS);
        runAndVerify(input);
    }

    private void runAndVerify(MatchInput input) {
        MatchCommand finalStatus = runAndVerifyBulkMatch(input, this.getClass().getSimpleName());
        validateBulkMatchResult(finalStatus.getResultLocation());
    }

    private MatchInput prepareBulkMatchInput(String scenario) {
        MatchInput input = new MatchInput();
        // FIXME: Fake tenant will cause exception
        // org.hibernate.PropertyValueException: not-null property references a
        // null or transient value :
        // com.latticeengines.domain.exposed.workflow.WorkflowJob.tenant
        // Need to think more how to avoid creating tenant for every match test
        // and void conflict while running entity match test in parallel
        input.setTenant(TENANT);
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

    private Map<String, MatchInput.EntityKeyMap> prepareKeyMaps() {
        Map<String, MatchInput.EntityKeyMap> keyMaps = new HashMap<>();
        MatchInput.EntityKeyMap keyMap = new MatchInput.EntityKeyMap();
        keyMap.setBusinessEntity(BusinessEntity.Account.name());
        Map<MatchKey, List<String>> map = MatchKeyUtils.resolveKeyMap(Arrays.asList(FIELDS));
        map.put(MatchKey.SystemId, Arrays.asList(SFDC_ID, MKTO_ID));
        keyMap.setKeyMap(map);
        keyMap.setSystemIdPriority(Arrays.asList(SFDC_ID, MKTO_ID));
        keyMaps.put(BusinessEntity.Account.name(), keyMap);

        return keyMaps;
    }

    private InputBuffer prepareBulkData(String scenario) {
        HdfsPodContext.changeHdfsPodId(this.getClass().getSimpleName());
        cleanupAvroDir(hdfsPathBuilder.podDir().toString());
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
}
