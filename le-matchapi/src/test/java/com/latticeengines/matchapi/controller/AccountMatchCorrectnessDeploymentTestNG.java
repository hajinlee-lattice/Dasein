package com.latticeengines.matchapi.controller;

import static com.latticeengines.matchapi.testframework.AdvancedAccountMatchTestResultVerifier.COL_ACCOUNT_ID;
import static com.latticeengines.matchapi.testframework.AdvancedAccountMatchTestResultVerifier.COL_EXISTS_IN_UNIVERSE;
import static com.latticeengines.matchapi.testframework.AdvancedAccountMatchTestResultVerifier.COL_MKTO_ID;
import static com.latticeengines.matchapi.testframework.AdvancedAccountMatchTestResultVerifier.COL_SFDC_ID;
import static com.latticeengines.matchapi.testframework.AdvancedAccountMatchTestResultVerifier.COL_TEST_GRP_ID;
import static com.latticeengines.matchapi.testframework.AdvancedAccountMatchTestResultVerifier.COL_TEST_GRP_TYPE;
import static com.latticeengines.matchapi.testframework.AdvancedAccountMatchTestResultVerifier.COL_TEST_RECORD_ID;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.InputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.matchapi.testframework.AdvancedAccountMatchTestResultVerifier;
import com.latticeengines.matchapi.testframework.MatchapiDeploymentTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;

/**
 * Run 2 matches.
 *
 * 1st match is to build existing account universe (seed & lookup table).
 *
 * 2nd match is based on account universe built by 1st match, run another match,
 * analyze and verify match result
 *
 * dpltc deploy -a matchapi,workflowapi,metadata,eai,modeling
 */
public class AccountMatchCorrectnessDeploymentTestNG extends MatchapiDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(AccountMatchCorrectnessDeploymentTestNG.class);

    private static final String TENANT_ID = AccountMatchCorrectnessDeploymentTestNG.class.getSimpleName()
            + UUID.randomUUID().toString();
    private Tenant tenant = new Tenant(CustomerSpace.parse(TENANT_ID).toString());

    private static final String POD_ID = AccountMatchCorrectnessDeploymentTestNG.class.getSimpleName();

    private static final String FLD_ACCOUNT_ID = COL_ACCOUNT_ID;
    private static final String FLD_SFDC_ID = COL_SFDC_ID;
    private static final String FLD_MKTO_ID = COL_MKTO_ID;

    private static final String[] FIELDS = { COL_TEST_GRP_TYPE, COL_TEST_GRP_ID, COL_TEST_RECORD_ID, FLD_ACCOUNT_ID,
            FLD_SFDC_ID, FLD_MKTO_ID, "Name", "Domain", "DUNS", "Country", "State", "City", COL_EXISTS_IN_UNIVERSE };

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Inject
    private TenantService tenantService;

    @Inject
    private EntityRawSeedService entityRawSeedService;

    @Inject
    private EntityLookupEntryService entityLookupEntryService;

    @Inject
    private EntityMatchVersionService entityMatchVersionService;

    // FIXME: Disable all the deployment tests related to Entity Match to tune
    // Decision Graph in QA with PM
    @BeforeClass(groups = "deployment", enabled = false)
    public void init() {
        HdfsPodContext.changeHdfsPodId(POD_ID);
        cleanupAvroDir(hdfsPathBuilder.podDir().toString());

        tenant.setName(TENANT_ID);
        tenantService.registerTenant(tenant);
        // populate pid so that the tenant could be deleted in destroy()
        tenant = tenantService.findByTenantId(tenant.getId());
        log.info("Tenant ID: {}", tenant.getId());
    }

    @AfterClass(groups = "deployment", enabled = false)
    public void destroy() {
        tenantService.discardTenant(tenant);
        cleanupAvroDir(hdfsPathBuilder.podDir().toString());
    }

    @Test(groups = "deployment", enabled = false)
    public void test() {
        // instantiate test verifier
        AdvancedAccountMatchTestResultVerifier verifier = new AdvancedAccountMatchTestResultVerifier(tenant,
                BusinessEntity.Account.name(), entityRawSeedService, entityLookupEntryService,
                entityMatchVersionService);

        MatchInput input = prepareBulkMatchInput("accountmatchinput_builduniverse.avro");
        MatchCommand finalStatus = runAndVerifyBulkMatch(input, POD_ID);
        // set the accounts populated in the existing universe for verification of the
        // second match job (some of the records have to match to a specific entity ID
        // in the universe now)
        verifier.addExistingRecords(AvroUtils.iterator(yarnConfiguration, finalStatus.getResultLocation() + "/*.avro"));
        input = prepareBulkMatchInput("accountmatchinput_test.avro");
        finalStatus = runAndVerifyBulkMatch(input, POD_ID);
        String resultAvro = finalStatus.getResultLocation() + "/*.avro";
        String errorAvro = new Path(finalStatus.getResultLocation()).getParent().toString() + "/Error/*.avro";
        log.info("Result file: " + resultAvro);
        log.info("Error file: " + errorAvro);
        Iterator<GenericRecord> results = AvroUtils.iterator(yarnConfiguration, resultAvro);
        Iterator<GenericRecord> errors = AvroUtils.iterator(yarnConfiguration, errorAvro);
        verifier.addTestResults(results);
        verifier.addMatchErrorResults(errors);
        verifier.verify();
    }

    private MatchInput prepareBulkMatchInput(String inputFile) {
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
        input.setInputBuffer(prepareBulkData(inputFile));
        input.setUseDnBCache(true);
        input.setUseRemoteDnB(true);
        return input;
    }

    private Map<String, MatchInput.EntityKeyMap> prepareKeyMaps() {
        Map<String, MatchInput.EntityKeyMap> keyMaps = new HashMap<>();
        MatchInput.EntityKeyMap keyMap = new MatchInput.EntityKeyMap();
        Map<MatchKey, List<String>> map = MatchKeyUtils.resolveKeyMap(Arrays.asList(FIELDS));
        map.put(MatchKey.SystemId, Arrays.asList(FLD_ACCOUNT_ID, FLD_SFDC_ID, FLD_MKTO_ID));
        keyMap.setKeyMap(map);
        keyMaps.put(BusinessEntity.Account.name(), keyMap);
        return keyMaps;
    }

    private InputBuffer prepareBulkData(String inputFile) {
        String avroDir = hdfsPathBuilder.podDir().toString() + "/"
                + inputFile.substring(0, inputFile.length() - ".avro".length());
        cleanupAvroDir(avroDir);
        AvroInputBuffer inputBuffer = new AvroInputBuffer();
        inputBuffer.setAvroDir(avroDir);
        try {
            HdfsUtils.copyLocalResourceToHdfs(yarnConfiguration, "matchinput/" + inputFile, avroDir + "/" + inputFile);
        } catch (IOException e) {
            throw new RuntimeException("Fail to upload file " + inputFile + " to hdfs path " + avroDir);
        }
        return inputBuffer;
    }
}
