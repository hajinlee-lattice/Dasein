package com.latticeengines.matchapi.controller;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.ENTITY_ID_FIELD;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.ENTITY_NAME_FIELD;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.AvroInputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchResult;
import com.latticeengines.domain.exposed.datacloud.match.InputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.matchapi.testframework.MatchapiDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.security.exposed.service.TenantService;

/**
 * Mostly focus on entity bulk match end-to-end code path. Covers some but
 * limited correctness verification
 *
 * Account match correctness verification is in EntityMatchCorrectnessTestNG &
 * AccountMatchCorrectnessDeploymentTestNG
 *
 * dpltc deploy -a matchapi,workflowapi,metadata,eai,modeling
 */
public class AccountMatchDeploymentTestNG extends MatchapiDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(AccountMatchDeploymentTestNG.class);

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Inject
    private TenantService tenantService;

    @Inject
    private MatchProxy matchProxy;

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
            InterfaceName.CustomerAccountId.name(), //
            SFDC_ID, //
    };

    private static final String[] FIELDS_FETCHONLY = { //
            InterfaceName.EntityId.name(), //
    };

    private static final String[] FIELDS_LEAD_TO_ACCT = { //
            TEST_ID, //
            MatchKey.Domain.name(), //
            MatchKey.DUNS.name(), //
            MatchKey.Name.name(), //
            MatchKey.Country.name(), //
            MatchKey.State.name(), //
            MatchKey.City.name(), //
            InterfaceName.CustomerAccountId.name(), //
            MatchKey.Email.name(), //
    };

    private static final String[] FIELDS_LEAD_TO_ACCT_NOAID = { //
            TEST_ID, //
            MatchKey.DUNS.name(), //
            MatchKey.Domain.name(), //
            MatchKey.Name.name(), //
            MatchKey.Email.name(), //
    };

    private static final List<Class<?>> SCHEMA = new ArrayList<>(Collections.nCopies(FIELDS.length, String.class));

    private static final List<Class<?>> SCHEMA_FETCHONLY = new ArrayList<>(
            Collections.nCopies(FIELDS_FETCHONLY.length, String.class));

    private static final List<Class<?>> SCHEMA_LEAD_TO_ACCT = new ArrayList<>(
            Collections.nCopies(FIELDS_LEAD_TO_ACCT.length, String.class));

    private static final List<Class<?>> SCHEMA_LEAD_TO_ACCT_NOAID = new ArrayList<>(
            Collections.nCopies(FIELDS_LEAD_TO_ACCT_NOAID.length, String.class));

    /***************************************************************
     * TestId is designed with format C<CaseGroupId>_<CaseId>
     * eg. C0_01 means CaseId = 01 under group CaseGroupId = 0
     * Result verification has dependency on CaseGroupId in TestId
     ***************************************************************/

    /*********************************************************************
     * DATA_ALL_KEYS setup an Account universe while other data sets are
     * designed to match to this universe
     *********************************************************************/
    // TODO: Change to duns = 079942718 when parent duns feature is
    // enabled.
    // Schema:Domain, DUNS, Name, Country, State, City, AccountId, SfdcId
    private static final Object[][] DATA_ALL_KEYS = {
            { "C0_01", "google.com", "060902413", "google", "usa", "ca", "mountain view", "acc_id", "sfdc_id" }, //
            { "C0_02", "amazon.com", "884745530", "amazon", "usa", "washington", "seattle", "acc_id_02", "sfdc_id_02" }, //
    };

    /************************************************************************
     * DATA_PARTIAL_KEYS is designed to all match to C0_01 in DATA_ALL_KEYS
     ************************************************************************/
    // TODO: Change to duns = 079942718 when parent duns feature is
    // enabled
    // Schema: TestId, Domain, DUNS, Name, Country, State, City, AccountId,
    // SfdcId
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

            // case 4: system id
            { "C4_01", null, null, null, null, null, null, " acc_id", null }, //
            { "C4_02", null, null, null, null, null, null, null, "sfdc_id " }, //
            { "C4_03", null, null, null, null, null, null, " acc_id ", "\t sfdc_id \t" }, //

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


    /*************************************************************************
     * Designed to test match based on Account universe setup by DATA_ALL_KEYS
     * (published to serving store)
     *************************************************************************/
    // Schema: TestId, Domain, DUNS, Name, Country, State, City,
    // CustomerAccountId, Email
    private static final Object[][] DATA_LEAD_TO_ACCT = {
            // case 6: CustomerAccountId = AccountId in case C0_01 -- All
            // expected to return AccountId acc_id in case C0_01

            // keys besides AID all empty
            { "C6_01", null, null, null, null, null, null, "acc_id", null }, //

            // all keys match to case C0_01
            { "C6_02", "google.com", null, null, null, null, null, "acc_id", null }, //
            { "C6_03", null, "060902413", null, null, null, null, "acc_id", null }, //
            { "C6_04", null, null, "google", "usa", null, null, "acc_id", null }, //

            // AID match to case C0_01, other match keys match to case C0_02 or
            // don't match to any (AID is highest priority key which
            // decides match result) -- All expected to return AccountId acc_id
            // in case C0_01
            { "C6_05", "amazon.com", null, null, null, null, null, "acc_id", null }, //
            { "C6_06", null, "884745530", null, null, null, null, "acc_id", null }, //
            { "C6_07", null, null, "amazon", "usa", "washington", "seattle", "acc_id", null }, //
            { "C6_08", null, "uber.com", null, null, null, null, "acc_id", null }, //
            { "C6_09", null, null, "123456789", null, null, null, "acc_id", null }, //
            { "C6_10", null, null, null, "facebook", "usa", null, "acc_id", null }, //


            // case 7: CustomerAccountId is empty with other match keys match
            // with case C0_01 -- All expected to return AccountId acc_id in
            // case C0_01
            { "C7_01", "google.com", null, null, null, null, null, null, null }, //
            { "C7_02", null, "060902413", null, null, null, null, "", null }, //
            { "C7_03", null, null, "google", "usa", null, null, "   ", null }, //


            // case 8: CustomerAccountId != AccountId in case C0_01, but other
            // keys matched -- Expected to return anonymous AccountId, since
            // CustomerAccountId doesn't exist in Account universe
            { "C8_01", "google.com", null, null, null, null, null, "acc_id_nonexist", null }, //
            { "C8_02", null, "060902413", null, null, null, null, "acc_id_nonexist", null }, //
            { "C8_03", null, null, "google", "usa", null, null, "acc_id_nonexist", null }, //


            // case 9: CustomerAccountId != AccountId in case C0_01 with other
            // match keys don't match either -- Expected to return anonymous
            // AccountId, since CustomerAccountId doesn't exist in Account
            // universe
            { "C9_01", "uber.com", null, null, null, null, null, "acc_id_nonexist", null }, //
            { "C9_02", "uber.com", null, null, null, null, null, null, null }, //
            { "C9_03", null, "123456789", null, null, null, null, "acc_id_nonexist", null }, //
            { "C9_04", null, "123456789", null, null, null, null, "", null }, //
            { "C9_05", null, null, "facebook", "usa", null, null, "acc_id_nonexist", null }, //
            { "C9_06", null, null, "facebook", "usa", null, null, "   ", null }, //


            // case 10: Test multi-domain field matching.  For proper testing, AccountId cannot be specified.
            // Otherwise, AccountId will match first, and domain matching will not be exercised.
            // These tests should match the Google entry in the account universe with AccountId acc_id, ie. C0_01
            // sub-case 1: Test that email field has preference over domain.
            { "C10_01", "private@lattice-engines.com", null, null, null, null, null, null, "private@google.com" }, //
            // sub-case 2: Test that public emails are skipped in domain matching order.
            { "C10_02", "google.com", null, null, null, null, null, null, "public@hotmail.com" }, //
            // sub-case 3: Test that null email is successfully skipped.
            { "C10_03", "google.com", null, null, null, null, null, null, null }, //
            // sub-case 4: Test that non-parseable email is skipped.
            { "C10_04", "google.com", null, null, null, null, null, null, "blah blah blah not a domain" }, //
            // sub-case 5: Test that public domain restriction is not relaxed when not an email if company name is
            // provided.
            { "C10_05", "yahoo.com", null, "Google", "United States of America", null, null, null, "outlook.com" }, //


            // case 11: Test multi-domain field matching cases that result in no match to the account universe.
            // sub-case 1: Test that public domain restriction is relaxed when not in email format and no duns or name
            // is provided. Here yahoo.com will be used for match and not match Google's entry, C0_01.
            { "C11_01", "google.com", null, null, null, null, null, null, "yahoo.com" }, //
            // sub-case 2: Test that no match is found if only public email domains are provided.
            { "C11_02", "public@yahoo.com", null, null, null, null, null, null, "public@outlook.com" }, //
    };

    /****************************************************************************
     * Designed to test match based on Account universe setup by DATA_ALL_KEYS
     * (published to serving store) without AID in schema and with other match
     * keys simplified
     ****************************************************************************/
    // Schema: TestId, DUNS, Domain, Name, Email
    // Priority in current default decision graph of Account: DUNS -> Domain ->
    // Name
    private static final Object[][] DATA_LEAD_TO_ACCT_NOAID = {
            // case 12: keys match to case C0_01 -- All expected to return
            // AccountId acc_id in case C0_01

            // all keys match to case C0_01
            { "C12_01", "060902413", null, null, null }, //
            { "C12_02", null, "google.com", null, null }, //
            { "C12_03", null, null, "google", null }, //
            { "C12_04", null, null, "google", "google@google.com" }, //

            // higher priority keys match to case C0_01, while lower priority
            // keys match to case C0_02
            { "C12_05", "060902413", "amazon.com", null, "amazon@amazon.com" }, //
            { "C12_06", "060902413", null, "amazon", null }, //
            { "C12_07", "060902413", "amazon.com", "amazon", "amazon@amazon.com" }, //
            { "C12_08", null, "amazon.com", "amazon", "google@google.com" }, //
            { "C12_09", null, "google.com", "amazon", null }, //

            // higher priority keys match to case C0_01, while lower priority
            // keys match to nothing
            { "C12_10", "060902413", null, null, null}, //
            { "C12_11", "060902413", "domain_nonexist.com", null, null }, //
            { "C12_12", "060902413", null, "company_nonexist", null }, //
            { "C12_13", "060902413", "domain_nonexist.com", "company_nonexist", "name@domain_nonexist.com" }, //
            { "C12_14", null, null, "company_nonexist", "google@google.com" }, //


            // case 13: keys not match to any existing account -- All expected
            // to return anonymous AccountId
            { "C13_01", null, null, null, null }, //
            { "C13_02", "000000000", null, null, null }, //
            { "C13_03", null, "domain_nonexist.com", null, "name@domain_nonexist.com" }, //
            { "C13_04", null, null, "company_nonexist", null }, //
            { "C13_05", "000000000", "domain_nonexist.com", null, null }, //
            { "C13_06", "000000000", null, "company_nonexist", null }, //
            { "C13_07", null, "domain_nonexist.com", "company_nonexist", "name@domain_nonexist.com" }, //
            { "C13_08", "000000000", "domain_nonexist.com", "company_nonexist", "name@domain_nonexist.com" }, //
            // public@aol.com should match before google.com (public domain treated as normal is true)
            { "C13_09", null, "google.com", null, "public@aol.com" }, //
    };


    // prepare in the run time because it needs EntityId got from non-fetch-only
    // mode test
    private Object[][] dataFetchOnly;


    private static final String CASE_ALL_KEYS = "ALL_KEYS";
    private static final String CASE_PARTIAL_KEYS = "PARTIAL_KEYS";
    private static final String CASE_LEAD_TO_ACCT = "LEAD_TO_ACCT";
    private static final String CASE_LEAD_TO_ACCT_NOAID = "LEAD_TO_ACCT_NOAID";

    private String googleEntityId = null;

    // FIXME: Disable all the deployment tests related to Entity Match to tune
    // Decision Graph in QA with PM
    @BeforeClass(groups = "deployment", enabled = false)
    public void init() {
        HdfsPodContext.changeHdfsPodId(this.getClass().getSimpleName());
        cleanupAvroDir(hdfsPathBuilder.podDir().toString());

        tenant.setName(TENANT_ID);
        tenantService.registerTenant(tenant);
        // populate pid so that the tenant could be deleted in destroy()
        tenant = tenantService.findByTenantId(tenant.getId());
    }

    @AfterClass(groups = "deployment", enabled = false)
    public void destroy() {
        tenantService.discardTenant(tenant);
    }

    // One record with all match key populated
    @Test(groups = "deployment", priority = 1, enabled = false)
    public void testAllKeys() {
        MatchInput input = prepareBulkMatchInput(CASE_ALL_KEYS);
        runAndVerify(input, CASE_ALL_KEYS);
        publishBaseSet();
    }

    // Records with partial match key (extracted from match key of #1)
    // populated, should all match to same EntityId in #1
    @Test(groups = "deployment", priority = 2, enabled = false)
    public void testPartialKeys() {
        MatchInput input = prepareBulkMatchInput(CASE_PARTIAL_KEYS);
        runAndVerify(input, CASE_PARTIAL_KEYS);
    }

    // Use EntityId got from #1 to test fetch-only mode
    @Test(groups = "deployment", priority = 3, enabled = false)
    public void testFetchOnly() {
        MatchInput input = prepareBulkMatchInputFetchOnly();
        runAndVerify(input, null);
    }

    // In M28, it is no longer supported. Disable the test
    // Provide all the match keys to test Lead-to-Account match ---
    // Non-AllocateId mode for Account match and return AccountId
    @Test(groups = "deployment", priority = 4, enabled = false)
    public void testLeadToAcct() {
        MatchInput input = prepareBulkMatchInputLeadToAcct(CASE_LEAD_TO_ACCT, true, MatchKey.Email.name(),
                false);
        runAndVerify(input, CASE_LEAD_TO_ACCT);
    }

    // In M28, it is no longer supported. Disable the test
    // Provide patial match keys without AccountId to test Lead-to-Account match
    // --- Non-AllocateId mode for Account match and return AccountId
    @Test(groups = "deployment", priority = 5, enabled = false)
    public void testLeadToAcctNoAID() {
        MatchInput input = prepareBulkMatchInputLeadToAcct(CASE_LEAD_TO_ACCT_NOAID, false,
                MatchKey.Email.name(), true);
        runAndVerify(input, CASE_LEAD_TO_ACCT_NOAID);
    }

    private void publishBaseSet() {
        // No need to bump up version because test generates new test every time
        EntityPublishRequest request = new EntityPublishRequest();
        request.setEntity(BusinessEntity.Account.name());
        request.setSrcTenant(tenant);
        request.setDestTenant(tenant);
        request.setDestEnv(EntityMatchEnvironment.SERVING);
        request.setDestTTLEnabled(true);
        EntityPublishStatistics stats = matchProxy.publishEntity(request);
        Assert.assertEquals(stats.getSeedCount(), DATA_ALL_KEYS.length);
    }

    private void runAndVerify(MatchInput input, String scenario) {
        MatchCommand finalStatus = runAndVerifyBulkMatch(input, this.getClass().getSimpleName());
        int numNewAccounts = CASE_ALL_KEYS.equals(scenario) ? DATA_ALL_KEYS.length : 0;
        validateNewlyAllocatedAcct(finalStatus.getResultLocation(), numNewAccounts);

        if (CASE_ALL_KEYS.equals(scenario) || CASE_PARTIAL_KEYS.equals(scenario)) {
            validateAllocateAcctResult(finalStatus.getResultLocation());
        } else if (CASE_LEAD_TO_ACCT.equals(scenario) || CASE_LEAD_TO_ACCT_NOAID.equals(scenario)) {
            validateLeadToAcctResult(finalStatus, scenario);
        } else if (input.isFetchOnly()) {
            validateAcctMatchFetchOnlyResult(finalStatus.getResultLocation());
        } else {
            throw new IllegalArgumentException("Don't know how to validate match result");
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
        input.setOutputNewEntities(true);
        input.setEntityKeyMaps(
                prepareKeyMaps(FIELDS, new String[] { InterfaceName.CustomerAccountId.name(), SFDC_ID }, null));
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

    // Set emailField to the name of the email field, or to null/empty if no email field is needed.
    private MatchInput prepareBulkMatchInputLeadToAcct(String scenario, boolean mapSystemId, String emailField,
                                                       boolean publicDomainAsNormal) {
        String[] fields = null;
        if (CASE_LEAD_TO_ACCT.equals(scenario)) {
            fields = FIELDS_LEAD_TO_ACCT;
        } else if (CASE_LEAD_TO_ACCT_NOAID.equals(scenario)) {
            fields = FIELDS_LEAD_TO_ACCT_NOAID;
        } else {
            throw new IllegalArgumentException("Unrecognized test scenario: " + scenario);
        }

        MatchInput input = new MatchInput();
        input.setTenant(tenant);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        input.setPredefinedSelection(Predefined.LeadToAcct);
        input.setFields(Arrays.asList(fields));
        input.setSkipKeyResolution(true);
        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        input.setTargetEntity(BusinessEntity.Account.name());
        input.setAllocateId(false);
        if (publicDomainAsNormal) {
            input.setPublicDomainAsNormalDomain(true);
        }
        if (mapSystemId) {
            input.setEntityKeyMaps(
                    prepareKeyMaps(fields, new String[] { InterfaceName.CustomerAccountId.name() }, emailField));
        } else {
            input.setEntityKeyMaps(prepareKeyMaps(fields, new String[] {}, emailField));
        }
        input.setInputBuffer(prepareBulkData(scenario));
        input.setUseDnBCache(true);
        input.setUseRemoteDnB(true);
        return input;
    }

    private Map<String, MatchInput.EntityKeyMap> prepareKeyMaps(String[] fields, String[] systemIdFields, String
            emailField) {
        Map<String, MatchInput.EntityKeyMap> keyMaps = new HashMap<>();
        MatchInput.EntityKeyMap keyMap = new MatchInput.EntityKeyMap();
        Map<MatchKey, List<String>> map = MatchKeyUtils.resolveKeyMap(Arrays.asList(fields));
        if (systemIdFields.length > 0) {
            map.put(MatchKey.SystemId, Arrays.asList(systemIdFields));
        }
        if (StringUtils.isNotBlank(emailField)) {
            List<String> domainMatchKeys = map.get(MatchKey.Domain);
            // Add email field first in Domain MatchKey because it is highest priority for matching.
            domainMatchKeys.add(0, emailField);
        }
        keyMap.setKeyMap(map);
        keyMaps.put(BusinessEntity.Account.name(), keyMap);

        return keyMaps;
    }

    private Map<String, MatchInput.EntityKeyMap> prepareKeyMapsFetchOnly() {
        Map<String, MatchInput.EntityKeyMap> keyMaps = new HashMap<>();
        MatchInput.EntityKeyMap keyMap = new MatchInput.EntityKeyMap();
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
        case CASE_LEAD_TO_ACCT:
            uploadAvroData(DATA_LEAD_TO_ACCT, Arrays.asList(FIELDS_LEAD_TO_ACCT), SCHEMA_LEAD_TO_ACCT, avroDir,
                    CASE_LEAD_TO_ACCT + ".avro");
            break;
        case CASE_LEAD_TO_ACCT_NOAID:
            uploadAvroData(DATA_LEAD_TO_ACCT_NOAID, Arrays.asList(FIELDS_LEAD_TO_ACCT_NOAID), SCHEMA_LEAD_TO_ACCT_NOAID,
                    avroDir, CASE_LEAD_TO_ACCT_NOAID + ".avro");
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
                { googleEntityId }, //
                { "FakedEntityId" }, //
                { null }, //
        };
        uploadAvroData(dataFetchOnly, Arrays.asList(FIELDS_FETCHONLY), SCHEMA_FETCHONLY, avroDir, "FETCH_ONLY.avro");
        return inputBuffer;
    }

    // Designed test case that all of them should match to googleEntityId
    private void validateAllocateAcctResult(String path) {
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, path + "/*.avro");
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            // case group 0 is to build universe
            if ("0".equals(extractCaseGroup(record.get(TEST_ID).toString()))) {
                Assert.assertNotNull(record.get(InterfaceName.EntityId.name()).toString());
                if ("C0_01".equals(record.get(TEST_ID).toString())) {
                    googleEntityId = record.get(InterfaceName.EntityId.name()).toString();
                }
            } else {
                Assert.assertEquals(record.get(InterfaceName.EntityId.name()).toString(), googleEntityId);
            }
        }
    }

    // check whether newly allocated account records match the expected number and
    // has entityName & entityId field
    private void validateNewlyAllocatedAcct(String outputPath, int numNewAccounts) {
        String avroGlob = PathUtils.toAvroGlob(outputPath);
        Long fileCount = fileCount(avroGlob);
        if (numNewAccounts == 0) {
            // should have no file if there are no new accounts
            Assert.assertEquals(fileCount.longValue(), 0L);
            return;
        } else {
            Assert.assertTrue(fileCount > 0, "Should have new entity avro file in path: " + outputPath);
        }
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, avroGlob);
        int total = 0;
        for (int i = 0; records.hasNext(); i++) {
            GenericRecord record = records.next();
            Assert.assertNotNull(record, String.format("Got null avro record at index = %d", i));
            Assert.assertNotNull(record.get(ENTITY_NAME_FIELD), getMissingFieldErrorMsg(ENTITY_NAME_FIELD, i));
            Assert.assertNotNull(record.get(ENTITY_ID_FIELD), getMissingFieldErrorMsg(ENTITY_ID_FIELD, i));
            total++;
        }
        Assert.assertEquals(total, numNewAccounts);
    }

    private long fileCount(String avroGlob) {
        try {
            Long count = AvroUtils.count(yarnConfiguration, avroGlob);
            return count == null ? 0 : count;
        } catch (IllegalArgumentException e) {
            // directory not exist
            return 0;
        }
    }

    private String getMissingFieldErrorMsg(String field, int recordIdx) {
        return String.format("Missing field %s in record at index %d", field, recordIdx);
    }

    private void validateAcctMatchFetchOnlyResult(String path) {
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, path + "/*.avro");
        int count = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            count++;
            log.info(record.toString());
            String entityId = record.get(InterfaceName.EntityId.name()) == null ? null
                    : record.get(InterfaceName.EntityId.name()).toString();
            Assert.assertNotNull(googleEntityId);
            if (googleEntityId.equals(entityId)) {
                Assert.assertNotNull(record.get(InterfaceName.LatticeAccountId.name()));
            } else {
                Assert.assertNull(record.get(InterfaceName.LatticeAccountId.name()));
            }
        }
        Assert.assertEquals(count, dataFetchOnly.length);
    }

    private void validateLeadToAcctResult(MatchCommand finalStatus, String scenario) {
        Set<String> casesMatchedAID;
        Set<String> casesAnonymousAID;
        if (CASE_LEAD_TO_ACCT.equals(scenario)) {
            casesMatchedAID = new HashSet<>(Arrays.asList("6", "7", "10"));
            casesAnonymousAID = new HashSet<>(Arrays.asList("8", "9", "11"));
        } else if (CASE_LEAD_TO_ACCT_NOAID.equals(scenario)) {
            casesMatchedAID = new HashSet<>(Collections.singletonList("12"));
            casesAnonymousAID = new HashSet<>(Collections.singletonList("13"));
        } else {
            throw new IllegalArgumentException("Unrecognized test scenario: " + scenario);
        }

        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, finalStatus.getResultLocation() + "/*.avro");
        int count = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            count++;
            log.info(record.toString());
            Assert.assertNotNull(record.get(InterfaceName.AccountId.name()));
            String groupId = extractCaseGroup(record.get(TEST_ID).toString());
            String acctId = record.get(InterfaceName.AccountId.name()).toString();
            if (casesMatchedAID.contains(groupId)) {
                Assert.assertEquals(acctId, "acc_id");
            } else if (casesAnonymousAID.contains(groupId)) {
                Assert.assertEquals(acctId, DataCloudConstants.ENTITY_ANONYMOUS_AID);
            } else {
                throw new IllegalArgumentException("Unrecognized test case group " + groupId);
            }
        }

        Map<EntityMatchResult, Long> matchResultMap = finalStatus.getMatchResults();
        log.info("Lead to Account Match Results for Scenario: " + scenario + ":");
        if (MapUtils.isEmpty(matchResultMap)) {
            log.info("   NO ENTITY MATCH RESULTS!");
        } else {
            for (Map.Entry<EntityMatchResult, Long> entry : matchResultMap.entrySet()) {
                log.info("   " + entry.getKey().name() + ": " + entry.getValue().toString());
            }
        }

        if (CASE_LEAD_TO_ACCT.equals(scenario)) {
            Assert.assertEquals(count, DATA_LEAD_TO_ACCT.length);

            // Validate MatchCommand Match Results.
            Assert.assertTrue(matchResultMap.containsKey(EntityMatchResult.ORPHANED_NO_MATCH));
            Assert.assertEquals(matchResultMap.get(EntityMatchResult.ORPHANED_NO_MATCH).longValue(), 8L);
            Assert.assertTrue(matchResultMap.containsKey(EntityMatchResult.ORPHANED_UNMATCHED_ACCOUNTID));
            Assert.assertEquals(matchResultMap.get(EntityMatchResult.ORPHANED_UNMATCHED_ACCOUNTID).longValue(), 3L);
            Assert.assertTrue(matchResultMap.containsKey(EntityMatchResult.MATCHED_BY_MATCHKEY));
            Assert.assertEquals(matchResultMap.get(EntityMatchResult.MATCHED_BY_MATCHKEY).longValue(), 8L);
            Assert.assertTrue(matchResultMap.containsKey(EntityMatchResult.MATCHED_BY_ACCOUNTID));
            Assert.assertEquals(matchResultMap.get(EntityMatchResult.MATCHED_BY_ACCOUNTID).longValue(), 10L);
        }
        if (CASE_LEAD_TO_ACCT_NOAID.equals(scenario)) {
            Assert.assertEquals(count, DATA_LEAD_TO_ACCT_NOAID.length);

            // Validate MatchCommand Match Results.
            Assert.assertTrue(matchResultMap.containsKey(EntityMatchResult.ORPHANED_NO_MATCH));
            Assert.assertEquals(matchResultMap.get(EntityMatchResult.ORPHANED_NO_MATCH).longValue(), 9L);
            Assert.assertTrue(matchResultMap.containsKey(EntityMatchResult.ORPHANED_UNMATCHED_ACCOUNTID));
            Assert.assertEquals(matchResultMap.get(EntityMatchResult.ORPHANED_UNMATCHED_ACCOUNTID).longValue(), 0L);
            Assert.assertTrue(matchResultMap.containsKey(EntityMatchResult.MATCHED_BY_MATCHKEY));
            Assert.assertEquals(matchResultMap.get(EntityMatchResult.MATCHED_BY_MATCHKEY).longValue(), 14L);
            Assert.assertTrue(matchResultMap.containsKey(EntityMatchResult.MATCHED_BY_ACCOUNTID));
            Assert.assertEquals(matchResultMap.get(EntityMatchResult.MATCHED_BY_ACCOUNTID).longValue(), 0L);
        }
    }

    private String extractCaseGroup(String caseId) {
        return caseId.substring(1, caseId.indexOf("_"));
    }
}
