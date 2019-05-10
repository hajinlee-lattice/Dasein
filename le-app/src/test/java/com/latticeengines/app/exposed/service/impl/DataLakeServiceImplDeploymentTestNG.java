package com.latticeengines.app.exposed.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;

/**
 * $ dpltc deploy -a admin,matchapi,pls,metadata,cdl,lp,objectapi
 */
public class DataLakeServiceImplDeploymentTestNG extends DataLakeServiceImplDeploymentTestNGBase {

    @Inject
    private LookupIdMappingProxy lookupIdMappingProxy;

    @Mock
    private MatchProxy mockedMatchProxyWithMatchedResult;

    @Mock
    private MatchProxy mockedMatchProxyWithNoMatchResult;

    private Map<String, String> orgInfo;
    private Map<String, String> orgInfo2;
    String accountIdColumn = InterfaceName.SalesforceAccountID.name();
    String accountIdColumn2 = InterfaceName.AccountId.name();

    private final List<String> expectedResultFields = Arrays.asList("Field1", "Field2");

    private final List<Object> expectedResultFieldValues = Arrays.asList("abc", new Long(123));

    private final List<String> expectedResultFieldsWithAccountId = Arrays.asList("Field1", "Field2",
            InterfaceName.AccountId.name());

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        mainTestTenant = testBed.getMainTestTenant();
        MultiTenantContext.setTenant(mainTestTenant);

        setupRedshiftData();

        MockitoAnnotations.initMocks(this);

        orgInfo = setupLookupIdMapping(accountIdColumn);
        orgInfo2 = setupLookupIdMapping(accountIdColumn2);
    }

    private Map<String, String> setupLookupIdMapping(String accountIdColumn) {
        Long timestamp = System.currentTimeMillis();
        String orgId = "O_" + timestamp;
        String orgName = "Name O_" + timestamp;

        LookupIdMap lookupIdMap = new LookupIdMap();
        lookupIdMap.setExternalSystemType(CDLExternalSystemType.CRM);
        lookupIdMap.setOrgId(orgId);
        lookupIdMap.setOrgName(orgName);

        lookupIdMap = lookupIdMappingProxy.registerExternalSystem(mainTestTenant.getId(), lookupIdMap);
        Assert.assertNotNull(lookupIdMap.getId());
        Assert.assertNull(lookupIdMap.getAccountId());

        lookupIdMap.setAccountId(accountIdColumn);
        lookupIdMap = lookupIdMappingProxy.updateLookupIdMap(mainTestTenant.getId(), lookupIdMap.getId(), lookupIdMap);
        Assert.assertNotNull(lookupIdMap.getAccountId());
        Assert.assertEquals(lookupIdMap.getAccountId(), accountIdColumn);

        lookupIdMap = lookupIdMappingProxy.getLookupIdMap(mainTestTenant.getId(), lookupIdMap.getId());
        Assert.assertNotNull(lookupIdMap);
        Assert.assertNotNull(lookupIdMap.getAccountId());
        Assert.assertEquals(lookupIdMap.getAccountId(), accountIdColumn);

        Map<String, String> orgInfo = new HashMap<>();
        orgInfo.put(CDLConstants.ORG_ID, orgId);
        orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, CDLExternalSystemType.CRM.name());

        return orgInfo;
    }

    private void mockWithGoodMatchResult() {
        createMockedMatchProxy(mockedMatchProxyWithMatchedResult, true);
        ((DataLakeServiceImpl) dataLakeService).setMatchProxy(mockedMatchProxyWithMatchedResult);
    }

    private void mockWithNoMatchResult() {
        createMockedMatchProxy(mockedMatchProxyWithNoMatchResult, false);
        ((DataLakeServiceImpl) dataLakeService).setMatchProxy(mockedMatchProxyWithNoMatchResult);
    }

    private void createMockedMatchProxy(MatchProxy mockedMatchProxy, boolean shouldCreateGoodResult) {
        MatchOutput matchOutput = new MatchOutput(UUID.randomUUID().toString());
        matchOutput.setOutputFields(expectedResultFields);
        OutputRecord outputRecord = new OutputRecord();
        if (shouldCreateGoodResult) {
            // outputRecord.setMatched(true);
            outputRecord.setOutput(expectedResultFieldValues);
        }
        List<OutputRecord> result = Arrays.asList(outputRecord);
        matchOutput.setResult(result);
        when(mockedMatchProxy.matchRealTime(any(MatchInput.class))).thenReturn(matchOutput);
    }

    @Test(groups = "deployment", enabled = true)
    public void testFindLookupIdColumn() {
        String lookupIdColumn = lookupIdMappingProxy.findLookupIdColumn(orgInfo, mainTestTenant.getId());
        Assert.assertNotNull(lookupIdColumn);
        Assert.assertEquals(lookupIdColumn, accountIdColumn);

        String lookupIdColumn2 = lookupIdMappingProxy.findLookupIdColumn(orgInfo2, mainTestTenant.getId());
        Assert.assertNotNull(lookupIdColumn2);
        Assert.assertEquals(lookupIdColumn2, accountIdColumn2);

        String lookupIdColumn3 = lookupIdMappingProxy.findLookupIdColumn(null, mainTestTenant.getId());
        Assert.assertNotNull(lookupIdColumn3);
        Assert.assertEquals(lookupIdColumn3, InterfaceName.SalesforceAccountID.name());
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testFindLookupIdColumn" })
    public void testGetAccountByIdWithNoMatch() {
        mockWithNoMatchResult();
        DataPage result = dataLakeService.getAccountById("someBadId", Predefined.TalkingPoint, orgInfo);
        checkResult(result, false, "someBadId");
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testGetAccountByIdWithNoMatch" })
    public void testGetAccountByIdWithActualInternalAccountId() {
        mockWithGoodMatchResult();
        DataPage result = dataLakeService.getAccountById(actualInternalAccountId, Predefined.TalkingPoint, orgInfo);
        checkResult(result, true, actualInternalAccountId);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testGetAccountByIdWithActualInternalAccountId" })
    public void testGetAccountByIdWithActualSfdcAccountId() {
        mockWithGoodMatchResult();
        DataPage result = dataLakeService.getAccountById(actualSfdcAccountId, Predefined.TalkingPoint, orgInfo);
        checkResult(result, true, actualInternalAccountId);
    }

    private void checkResult(DataPage result, boolean shouldExpectMathcedResult, String expectedAccountId) {
        Assert.assertNotNull(result);
        if (shouldExpectMathcedResult) {
            System.out.println("\n\n" + JsonUtils.serialize(result) + "\n\n");

            Assert.assertNotNull(result.getData());
            Assert.assertTrue(CollectionUtils.isNotEmpty(result.getData()));
            Map<String, Object> resultRecordValueMap = result.getData().get(0);
            Assert.assertTrue(MapUtils.isNotEmpty(resultRecordValueMap));
            Assert.assertEquals(resultRecordValueMap.size(), expectedResultFieldsWithAccountId.size());
            resultRecordValueMap.keySet().stream() //
                    .forEach(k -> Assert.assertTrue(expectedResultFieldsWithAccountId.contains(k), k));
            resultRecordValueMap.values().stream() //
                    .forEach(v -> {
                        if (!expectedResultFieldValues.contains(v)) {
                            Assert.assertEquals(v, expectedAccountId);
                        } else {
                            Assert.assertTrue(expectedResultFieldValues.contains(v), v == null ? null : v.toString());
                        }
                    });
        } else {
            Assert.assertNotNull(result.getData());
            Assert.assertTrue(result.getData().isEmpty());
        }
    }

    @Test(groups = "deployment")
    public void testGetAttributes() {
        testAndVerifyGetAttributes();
    }

    @Override
    protected Set<Pair<String, Category>> getExpectedAttrs() {
        return ImmutableSet.of( //
                Pair.of(InterfaceName.AccountId.name(), Category.ACCOUNT_ATTRIBUTES), //
                Pair.of(InterfaceName.ContactId.name(), Category.CONTACT_ATTRIBUTES));
    }

    @Override
    protected Set<Pair<String, Category>> getUnexpectedAttrs() {
        return ImmutableSet.of( //
                Pair.of(InterfaceName.AccountId.name(), Category.CONTACT_ATTRIBUTES));
    }
}
