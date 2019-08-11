package com.latticeengines.playmaker.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.latticeengines.playmakercore.service.EntityQueryGenerator;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-testframework-cleanup-context.xml",
        "classpath:playmakercore-context.xml", "classpath:test-playmaker-context.xml" })
public class LpiPMAccountExtensionImplDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(LpiPMAccountExtensionImplDeploymentTestNG.class);

    private LpiPMAccountExtensionImpl lpiPMAccountExtensionImpl;

    @Inject
    private TestPlayCreationHelper testPlayCreationHelper;

    @Inject
    private EntityQueryGenerator entityQueryGenerator;

    @Inject
    private LookupIdMappingProxy lookupIdMappingProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private BatonService batonService;

    @Mock
    private MatchProxy mockedMatchProxyWithMatchedResult;

    private long accountCount;

    private List<String> internalAccountIds;

    private List<String> sfdcAccountIds;

    private Map<String, String> orgInfo;

    private List<List<String>> expectedResultFields = Arrays
            .asList(Arrays.asList("AccountId", "CDLUpdatedTime", "SalesforceAccountID"), Arrays.asList( //
                    "LatticeAccountId", //
                    "CDLUpdatedTime", //
                    "AccountId", //
                    "Website", //
                    "LDC_Name", //
                    "ID", //
                    "LEAccountExternalID", //
                    "LastModificationDate", //
                    "SalesforceAccountID", //
                    "RowNum"));

    private List<List<Object>> expectedResultFieldValues = Arrays
            .asList(Arrays.asList("abc", "abc", "abc", new Long(123), new Long(123), 1, 1), Arrays.asList( //
                    "123", //
                    new Date(), //
                    "1", //
                    "www.website.com", //
                    "LDC_Name", //
                    "123", //
                    "123", //
                    new Date(), //
                    "123", //
                    1));

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        testPlayCreationHelper.setupTenantAndData();
        EntityProxy entityProxy = testPlayCreationHelper.initEntityProxy();

        orgInfo = setupLookupIdMapping(PlaymakerConstants.SfdcAccountID, testPlayCreationHelper.getTenant().getId());

        lpiPMAccountExtensionImpl = new LpiPMAccountExtensionImpl();
        lpiPMAccountExtensionImpl.setEntityProxy(entityProxy);
        lpiPMAccountExtensionImpl.setEntityQueryGenerator(entityQueryGenerator);
        lpiPMAccountExtensionImpl.setLookupIdMappingProxy(lookupIdMappingProxy);
        lpiPMAccountExtensionImpl.setColumnMetadataProxy(columnMetadataProxy);
        lpiPMAccountExtensionImpl.setBatonService(batonService);

        MockitoAnnotations.initMocks(this);

        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        frontEndQuery.addLookups(BusinessEntity.Account, InterfaceName.AccountId.name(),
                InterfaceName.SalesforceAccountID.name());
        PageFilter pageFilter = new PageFilter(0L, 5L);
        frontEndQuery.setPageFilter(pageFilter);

        DataPage result = entityProxy.getDataFromObjectApi(testPlayCreationHelper.getTenant().getId(), frontEndQuery);
        Assert.assertNotNull(result);
        Assert.assertTrue(CollectionUtils.isNotEmpty(result.getData()));
        Assert.assertEquals(result.getData().size(), 5);
        internalAccountIds = new ArrayList<>();
        sfdcAccountIds = new ArrayList<>();
        result.getData().forEach(row -> {
            Assert.assertTrue(row.containsKey(InterfaceName.AccountId.name()));
            Assert.assertTrue(row.containsKey(InterfaceName.SalesforceAccountID.name()));
            String actualInternalAccountId = row.get(InterfaceName.AccountId.name()).toString();
            String actualSfdcAccountId = row.get(InterfaceName.SalesforceAccountID.name()).toString();
            internalAccountIds.add(actualInternalAccountId);
            sfdcAccountIds.add(actualSfdcAccountId);
        });
    }

    @Test(groups = "deployment")
    public void testGetAccountExtensionCount() {
        accountCount = lpiPMAccountExtensionImpl.getAccountExtensionCount(0L, null, null, 0L, orgInfo);
        Assert.assertTrue(accountCount > 0);
    }

    private Map<String, String> setupLookupIdMapping(String accountIdColumn, String tenantId) {
        Long timestamp = System.currentTimeMillis();
        String orgId = "O_" + timestamp;
        String orgName = "Name O_" + timestamp;

        LookupIdMap lookupIdMap = new LookupIdMap();
        lookupIdMap.setExternalSystemType(CDLExternalSystemType.CRM);
        lookupIdMap.setOrgId(orgId);
        lookupIdMap.setOrgName(orgName);

        lookupIdMap = lookupIdMappingProxy.registerExternalSystem(tenantId, lookupIdMap);
        Assert.assertNotNull(lookupIdMap.getId());
        Assert.assertNull(lookupIdMap.getAccountId());

        lookupIdMap.setAccountId(accountIdColumn);
        lookupIdMap = lookupIdMappingProxy.updateLookupIdMap(tenantId, lookupIdMap.getId(), lookupIdMap);
        Assert.assertNotNull(lookupIdMap.getAccountId());
        Assert.assertEquals(lookupIdMap.getAccountId(), accountIdColumn);

        lookupIdMap = lookupIdMappingProxy.getLookupIdMap(tenantId, lookupIdMap.getId());
        Assert.assertNotNull(lookupIdMap);
        Assert.assertNotNull(lookupIdMap.getAccountId());
        Assert.assertEquals(lookupIdMap.getAccountId(), accountIdColumn);

        Map<String, String> orgInfo = new HashMap<>();
        orgInfo.put(CDLConstants.ORG_ID, orgId);
        orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, CDLExternalSystemType.CRM.name());

        return orgInfo;
    }

    // @Test(groups = "deployment")
    // public void testGetAccountExtensionCount() {
    // accountCount = lpiPMAccountExtensionImpl.getAccountExtensionCount(0L,
    // null, 0L);
    // Assert.assertTrue(accountCount > 0);
    // }

    @Test(groups = "deployment", dependsOnMethods = { "testGetAccountExtensionCount" })
    public void testGetAccountExtensionsWithoutAccountIds() throws Exception {
        log.info("testGetAccountExtensionsWithoutAccountIds");
        long max = accountCount > 5L ? 5L : accountCount;
        lpiPMAccountExtensionImpl.setMatchProxy(mockedMatchProxyWithMatchedResult);
        createMockedMatchProxy(mockedMatchProxyWithMatchedResult, expectedResultFields.get(0),
                expectedResultFieldValues.get(0), true, (int) max);
        List<Map<String, Object>> datapage = //
                lpiPMAccountExtensionImpl.getAccountExtensions( //
                        0, 0, max, null, null, //
                        null, null, false, null);
        log.info(datapage.toString());

        checkResult(datapage, expectedResultFields.get(0), expectedResultFieldValues.get(0), max);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetAccountExtensionsWithoutAccountIds" })
    public void testGetAccountExtensionsWithoutColumns() throws Exception {
        log.info("testGetAccountExtensionsWithoutColumns");
        long max = accountCount > 5L ? 5L : accountCount;
        lpiPMAccountExtensionImpl.setMatchProxy(mockedMatchProxyWithMatchedResult);
        createMockedMatchProxy(mockedMatchProxyWithMatchedResult, expectedResultFields.get(0),
                expectedResultFieldValues.get(0), true, (int) max);
        List<Map<String, Object>> datapage = //
                lpiPMAccountExtensionImpl.getAccountExtensions( //
                        0, 0, max, internalAccountIds, null, //
                        null, null, false, orgInfo);
        log.info(datapage.toString());

        checkResult(datapage, expectedResultFields.get(0), expectedResultFieldValues.get(0), max);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetAccountExtensionsWithoutColumns" })
    public void testGetAccountExtensionsWithColumns() {
        log.info("testGetAccountExtensionsWithColumns");
        long max = accountCount > 5L ? 5L : accountCount;
        lpiPMAccountExtensionImpl.setMatchProxy(mockedMatchProxyWithMatchedResult);
        createMockedMatchProxy(mockedMatchProxyWithMatchedResult, expectedResultFields.get(1),
                expectedResultFieldValues.get(1), true, (int) max);
        List<Map<String, Object>> datapage = //
                lpiPMAccountExtensionImpl.getAccountExtensions( //
                        0, 0, max, internalAccountIds, null, null, //
                        String.join(",", expectedResultFields.get(1)), false, null);
        log.info(datapage.toString());

        checkResult(datapage, expectedResultFields.get(1), expectedResultFieldValues.get(1), max);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetAccountExtensionsWithColumns" })
    public void testGetAccountExtensionsWithLookupIdColumn() throws Exception {
        log.info("testGetAccountExtensionsWithLookupIdColumn");
        long max = accountCount > 5L ? 5L : accountCount;
        lpiPMAccountExtensionImpl.setMatchProxy(mockedMatchProxyWithMatchedResult);
        createMockedMatchProxy(mockedMatchProxyWithMatchedResult, expectedResultFields.get(1),
                expectedResultFieldValues.get(1), true, (int) max);
        List<Map<String, Object>> datapage = //
                lpiPMAccountExtensionImpl.getAccountExtensions( //
                        0, 0, max, internalAccountIds, null, null, //
                        String.join(",", expectedResultFields.get(1)), false, orgInfo);
        log.info(datapage.toString());

        checkResult(datapage, expectedResultFields.get(1), expectedResultFieldValues.get(1), max);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetAccountExtensionsWithColumns" })
    public void testGetAccountExtensionsWithoutMatch() throws Exception {
        log.info("testGetAccountExtensionsWithoutMatch");
        long max = accountCount > 5L ? 5L : accountCount;
        lpiPMAccountExtensionImpl.setMatchProxy(mockedMatchProxyWithMatchedResult);
        List<Object> emptyResultsList = new ArrayList<Object>();
        createMockedMatchProxy(mockedMatchProxyWithMatchedResult, expectedResultFields.get(1), emptyResultsList, false,
                (int) max);
        List<Map<String, Object>> datapage = //
                lpiPMAccountExtensionImpl.getAccountExtensions( //
                        0, 0, max, Arrays.asList("id1", "id2", "id3", "id4", "id5"), null, null, //
                        String.join(",", expectedResultFields.get(1)), false, orgInfo);
        log.info(datapage.toString());

        checkResult(datapage, expectedResultFields.get(1), emptyResultsList, 0);
    }

    private void createMockedMatchProxy(MatchProxy mockedMatchProxy, List<String> expectedResultFields,
            List<Object> expectedResultFieldValues, boolean shouldCreateGoodResult, int maximum) {
        MatchOutput matchOutput = new MatchOutput(UUID.randomUUID().toString());
        matchOutput.setOutputFields(expectedResultFields);
        ArrayList<OutputRecord> result = new ArrayList<OutputRecord>();
        IntStream.range(0, maximum).forEach(i -> {
            OutputRecord outputRecord = new OutputRecord();
            if (shouldCreateGoodResult) {
                outputRecord.setOutput(expectedResultFieldValues);
                result.add(outputRecord);
            }
        });
        matchOutput.setResult(result);
        when(mockedMatchProxy.matchRealTime(any(MatchInput.class))).thenReturn(matchOutput);
    }

    private void checkResult(List<Map<String, Object>> datapage, List<String> expectedResultFields,
            List<Object> expectedResultFieldValues, long max) {
        Assert.assertNotNull(datapage);
        Assert.assertEquals(datapage.size(), (int) max);

        datapage.stream() //
                .forEach(row -> {
                    if (row.keySet().size() != expectedResultFieldValues.size()) {
                        log.info(String.format("Expected fields: %s , Result fields: %s", //
                                JsonUtils.serialize(expectedResultFieldValues), //
                                JsonUtils.serialize(row.keySet())));
                        log.info("Account Ext Data: " + JsonUtils.serialize(row));
                    }
                    log.info(JsonUtils.serialize(row.keySet()));
                    Assert.assertEquals(row.keySet().size(), expectedResultFieldValues.size());
                    Assert.assertNotNull(row.get(InterfaceName.AccountId.name()));
                    Assert.assertNotNull(row.get(PlaymakerConstants.ID));
                    Assert.assertNotNull(row.get(PlaymakerConstants.LEAccountExternalID));
                    Assert.assertNotNull(row.get(PlaymakerRecommendationEntityMgr.LAST_MODIFIATION_DATE_KEY));
                    Assert.assertNotNull(row.get(PlaymakerConstants.RowNum));
                    expectedResultFields.stream() //
                            .forEach(field -> {
                                if (!row.containsKey(field)) {
                                    log.info(String.format("Expected field: %s , Result fields: %s", //
                                            field, //
                                            JsonUtils.serialize(row.keySet())));
                                }

                                Assert.assertTrue(row.containsKey(field),
                                        String.format("row = %s, field = %s", JsonUtils.serialize(row), field));
                            });
                });
    }
}
