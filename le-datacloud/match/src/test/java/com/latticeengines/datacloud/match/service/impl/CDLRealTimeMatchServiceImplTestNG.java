package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.SERVICE_TENANT;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.SERVING;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.datacloud.core.service.impl.ZkConfigurationServiceImpl;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.service.EntityLookupEntryService;
import com.latticeengines.datacloud.match.service.EntityMatchVersionService;
import com.latticeengines.datacloud.match.service.EntityRawSeedService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestCDLMatchUtils;
import com.latticeengines.datacloud.match.testframework.TestEntityMatchService;
import com.latticeengines.datacloud.match.testframework.TestMatchInputService;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntryConverter;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({ SimpleRetryListener.class })
public class CDLRealTimeMatchServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String ACCOUNT_ID = "10";

    @Mock
    private ZkConfigurationServiceImpl zkConfigurationService;

    @Mock
    private DataCollectionProxy dataCollectionProxy;

    @Mock
    private ServingStoreProxy servingStoreProxy;

    @Inject
    private RealTimeMatchPlanner realTimeMatchPlanner;

    @Inject
    private RealTimeMatchService realTimeMatchService;

    @Inject
    private RealTimeEntityMatchPlanner realTimeEntityMatchPlanner;

    @Inject
    private TestMatchInputService testMatchInputService;

    @Inject
    private CDLLookupServiceImpl cdlColumnSelectionService;

    @Inject
    private TestEntityMatchService testEntityMatchService;

    @Inject
    private EntityRawSeedService entityRawSeedService;

    @Inject
    private EntityLookupEntryService entityLookupEntryService;

    @Inject
    private EntityMatchVersionService entityMatchVersionService;

    private Map<String, ColumnMetadata> accountSchema;

    private Map<String, ColumnMetadata> ratingSchema;

    private Map<String, ColumnMetadata> purchaseHistorySchema;

    private static final String CUSTOMER_ACCOUNT_ID = "c_aid_1";
    private static String[] accountAttrs = { "Website", "AlexaRank", "BmbrSurge_HumanResourceManagement_Intent" };
    private static Object[] accountAttrVals = { "aboitiz.com", 702905, null };
    private static String[] ratingAttrs = { "engine_mc7o9gwpq8gfw0wzkvekmw_score",
            "engine_zsujpzaatkoogxwx2zz8pa_score", "engine_mc7o9gwpq8gfw0wzkvekmw", "engine_zsujpzaatkoogxwx2zz8pa" };
    private static Object[] ratingAttrVals = { 5, 37, "D", "D" };
    private static String[] phAttrs = { "AM_650050C066EF46905EC469E9CC2921E0__EVER__HP",
            "AM_650050C066EF46905EC469E9CC2921E0__Q_1__Q_2_3__SC", "AM_650050C066EF46905EC469E9CC2921E0__Q_1__AS" };
    private static Object[] phAttrVals = { true, 397, 123400.0 };

    @BeforeClass(groups = "functional")
    public void setup() {
        loadAccountSchema();
        loadRatingSchema();
        loadPurchaseHistorySchema();

        MockitoAnnotations.initMocks(this);
        when(zkConfigurationService.isCDLTenant(any())).thenReturn(true);
        when(servingStoreProxy.getDecoratedMetadataFromCache(anyString(), eq(Account))) //
                .thenReturn(Stream.of(//
                        accountSchema.get(accountAttrs[0]), //
                        accountSchema.get(accountAttrs[1]), //
                        accountSchema.get(accountAttrs[2])).peek(cm -> cm.setAttrState(AttrState.Active))
                        .collect(Collectors.toList()));
        when(servingStoreProxy.getDecoratedMetadataFromCache(anyString(), eq(BusinessEntity.Rating))) //
                .thenReturn(Stream.of(//
                        ratingSchema.get(ratingAttrs[0]), //
                        ratingSchema.get(ratingAttrs[1]), //
                        ratingSchema.get(ratingAttrs[2]), //
                        ratingSchema.get(ratingAttrs[3])) //
                        .peek(cm -> cm.setAttrState(AttrState.Active)).collect(Collectors.toList()));
        when(servingStoreProxy.getDecoratedMetadataFromCache(anyString(), eq(BusinessEntity.PurchaseHistory))) //
                .thenReturn(Stream.of(//
                        purchaseHistorySchema.get(phAttrs[0]), //
                        purchaseHistorySchema.get(phAttrs[1]), //
                        purchaseHistorySchema.get(phAttrs[2])) //
                        .peek(cm -> cm.setAttrState(AttrState.Active)).collect(Collectors.toList()));
        when(dataCollectionProxy.getDynamoDataUnits(anyString(), any(), any()))
                .thenReturn(TestCDLMatchUtils.mockDynamoDataUnits());

        realTimeMatchPlanner.setZkConfigurationService(zkConfigurationService);
        realTimeEntityMatchPlanner.setZkConfigurationService(zkConfigurationService);
        cdlColumnSelectionService.setServingStoreProxy(servingStoreProxy);
        cdlColumnSelectionService.setDataCollectionProxy(dataCollectionProxy);
        setupSeedLookupTable();
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testCDLLookupByAccountId() {
        Object[][] data = new Object[][] { { 123, ACCOUNT_ID } };
        MatchInput input = prepareMatchInput(data);
        MatchOutput output = realTimeMatchService.match(input);
        verifyAttributeInMatchOutput(output, true);
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class, dataProvider = "entityMatchAttrLookup")
    private void testEntityMatchAttrLookup(String accountId, String customerAccountId, MatchKey lookupIdMatchKey,
            boolean shouldMatch) {
        Object[] data = new Object[] { accountId, customerAccountId };
        MatchInput.EntityKeyMap keyMap = new MatchInput.EntityKeyMap();
        keyMap.addMatchKey(MatchKey.SystemId, InterfaceName.CustomerAccountId.name());
        keyMap.addMatchKey(lookupIdMatchKey, InterfaceName.AccountId.name());
        MatchInput input = prepareEntityLookupInput(data, keyMap);

        MatchOutput output = realTimeMatchService.match(input);
        verifyAttributeInMatchOutput(output, shouldMatch);
    }

    @DataProvider(name = "entityMatchAttrLookup")
    private Object[][] entityMatchAttrLookupTestData() {
        return new Object[][] { //
                /*-
                 * valid entity id
                 */
                { ACCOUNT_ID, null, MatchKey.EntityId, true }, //
                { ACCOUNT_ID, "", MatchKey.EntityId, true }, //
                { ACCOUNT_ID, CUSTOMER_ACCOUNT_ID, MatchKey.EntityId, true }, //
                { ACCOUNT_ID, "skdjfklsjskldkjfsl", MatchKey.EntityId, true }, //
                /*-
                 * missing entity id, valid customer account id
                 */
                { "", CUSTOMER_ACCOUNT_ID, MatchKey.EntityId, true }, //
                { null, CUSTOMER_ACCOUNT_ID, MatchKey.EntityId, true }, //
                /*-
                 * invalid entity ID, valid customer account id (skip match if given lookup ID doesn't exist)
                 */
                { "sldkjfklsjfdls", CUSTOMER_ACCOUNT_ID, MatchKey.EntityId, false }, //
                { "sldkjfklsjfdls", "", MatchKey.EntityId, false }, //
                { "sldkjfklsjfdls", null, MatchKey.EntityId, false }, //
                /*-
                 * missing entity ID, missing customer account id
                 */
                { "", null, MatchKey.EntityId, false }, //
                { null, null, MatchKey.EntityId, false }, //
        }; //
    }

    /*
     * Data: [ AccountId, CustomerAccount]. Specify key map for these fields.
     */
    private MatchInput prepareEntityLookupInput(Object[] data, MatchInput.EntityKeyMap map) {
        MatchInput input = new MatchInput();
        input.setTenant(new Tenant(SERVICE_TENANT));
        input.setDataCloudVersion(currentDataCloudVersion);
        input.setOperationalMode(OperationalMode.ENTITY_MATCH_ATTR_LOOKUP);
        input.setTargetEntity(Account.name());

        Map<String, MatchInput.EntityKeyMap> keyMap = new HashMap<>();
        keyMap.put(Account.name(), map);
        input.setEntityKeyMaps(keyMap);
        input.setData(Collections.singletonList(Arrays.asList(data)));

        input.setFields(Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.CustomerAccountId.name()));
        input.setSkipKeyResolution(true);
        input.setAllocateId(false);
        input.setDataCloudOnly(false);
        input.setPredefinedSelection(null);
        input.setCustomSelection(getColumnSelection());
        return input;
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testCDLLookupByAccountIdNoMatch() {
        Object[][] data = new Object[][] { { 123, "12345" } };
        MatchInput input = prepareMatchInput(data);
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertEquals(output.getResult().size(), 1);
        OutputRecord record = output.getResult().get(0);
        Assert.assertFalse(record.isMatched());
    }

    private void verifyAttributeInMatchOutput(MatchOutput output, boolean shouldMatch) {
        Assert.assertEquals(output.getResult().size(), 1);
        OutputRecord record = output.getResult().get(0);
        Assert.assertEquals(record.isMatched(), shouldMatch);
        Assert.assertNotNull(record.getOutput());
        if (shouldMatch) {
            int offset = 0;
            for (int i = 0; i < accountAttrs.length; i++) {
                Assert.assertEquals(record.getOutput().get(i + offset), accountAttrVals[i]);
            }
            offset += accountAttrs.length;
            for (int i = 0; i < ratingAttrs.length; i++) {
                Assert.assertEquals(record.getOutput().get(i + offset), ratingAttrVals[i]);
            }
            offset += ratingAttrs.length;
            for (int i = 0; i < phAttrs.length; i++) {
                Assert.assertEquals(record.getOutput().get(i + offset), phAttrVals[i]);
            }
        }
    }

    /*
     * input for no-entity match attribute lookup (CDL match)
     */
    private MatchInput prepareMatchInput(Object[][] data) {
        String[] fields = new String[] { "ID", InterfaceName.AccountId.name() };
        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data, fields);
        input.setKeyMap(ImmutableMap.of(MatchKey.LookupId, Collections.singletonList("AccountId")));
        input.setPredefinedSelection(null);
        input.setCustomSelection(getColumnSelection());
        input.setDataCloudVersion(currentDataCloudVersion);
        input.setOperationalMode(OperationalMode.CDL_LOOKUP);
        return input;
    }

    private ColumnSelection getColumnSelection() {
        ColumnSelection columnSelection = new ColumnSelection();
        List<Column> columns = new ArrayList<>();
        for (String attr : accountAttrs) {
            columns.add(new Column(attr));
        }
        for (String attr : ratingAttrs) {
            columns.add(new Column(attr));
        }
        for (String attr : phAttrs) {
            columns.add(new Column(attr));
        }
        columnSelection.setColumns(columns);
        return columnSelection;
    }

    /*
     * setup mapping for entity match
     */
    private void setupSeedLookupTable() {
        testEntityMatchService.bumpVersion(SERVICE_TENANT);

        Tenant tenant = new Tenant(SERVICE_TENANT);
        String entity = Account.name();

        EntityLookupEntry entry = EntityLookupEntryConverter.fromSystemId(entity,
                InterfaceName.CustomerAccountId.name(), CUSTOMER_ACCOUNT_ID);
        List<EntityLookupEntry> entries = Collections.singletonList(entry);
        EntityRawSeed seed = new EntityRawSeed(ACCOUNT_ID, entity, entries, null);
        entityRawSeedService.setIfNotExists(SERVING, tenant, seed, true,
                entityMatchVersionService.getCurrentVersion(SERVING, tenant));

        entityLookupEntryService.createIfNotExists(SERVING, tenant, entry, ACCOUNT_ID, true);
    }

    private void loadAccountSchema() {
        List<ColumnMetadata> cms = TestCDLMatchUtils.loadAccountSchema(Account);
        accountSchema = new HashMap<>();
        cms.forEach(cm -> accountSchema.put(cm.getAttrName(), cm));
    }

    private void loadRatingSchema() {
        List<ColumnMetadata> cms = TestCDLMatchUtils.loadAccountSchema(BusinessEntity.Rating);
        ratingSchema = new HashMap<>();
        cms.forEach(cm -> ratingSchema.put(cm.getAttrName(), cm));
    }

    private void loadPurchaseHistorySchema() {
        List<ColumnMetadata> cms = TestCDLMatchUtils.loadAccountSchema(BusinessEntity.PurchaseHistory);
        purchaseHistorySchema = new HashMap<>();
        cms.forEach(cm -> purchaseHistorySchema.put(cm.getAttrName(), cm));
    }

}
