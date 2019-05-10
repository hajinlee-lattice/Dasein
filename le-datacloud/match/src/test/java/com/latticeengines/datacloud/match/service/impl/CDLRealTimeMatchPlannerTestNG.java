package com.latticeengines.datacloud.match.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.service.impl.ZkConfigurationServiceImpl;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestCDLMatchUtils;
import com.latticeengines.datacloud.match.testframework.TestMatchInputService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

public class CDLRealTimeMatchPlannerTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String ACCOUNT_ID = "10";

    @Mock
    private ZkConfigurationServiceImpl zkConfigurationService;

    @Mock
    private ServingStoreProxy servingStoreProxy;

    @Mock
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private RealTimeMatchPlanner realTimeMatchPlanner;

    @Inject
    private CDLLookupServiceImpl cdlMetadataService;

    @Inject
    private TestMatchInputService testMatchInputService;

    private Map<String, ColumnMetadata> accountSchema;

    private Map<String, ColumnMetadata> ratingSchema;

    private Map<String, ColumnMetadata> purchaseHistorySchema;

    @BeforeClass(groups = { "functional", "manual" } )
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(zkConfigurationService.isCDLTenant(any())).thenReturn(true);

        realTimeMatchPlanner.setZkConfigurationService(zkConfigurationService);
        cdlMetadataService.setServingStoreProxy(servingStoreProxy);
        cdlMetadataService.setDataCollectionProxy(dataCollectionProxy);

        loadAccountSchema();
        loadRatingSchema();
        loadPurchaseHistorySchema();
    }

    @Test(groups = "functional")
    public void testParseMetadata() {
        when(servingStoreProxy.getDecoratedMetadataFromCache(anyString(), eq(BusinessEntity.Account))) //
                .thenReturn(Arrays.asList( //
                        accountSchema.get("AccountId"), //
                        accountSchema.get("Website"), //
                        accountSchema.get("City"), //
                        accountSchema.get("AdvertisingTechnologiesTopAttributes") //

        ));
        when(servingStoreProxy.getDecoratedMetadataFromCache(anyString(), eq(BusinessEntity.Rating))) //
                .thenReturn(Arrays.asList( //
                        ratingSchema.get("engine_mc7o9gwpq8gfw0wzkvekmw") //

        ));
        when(servingStoreProxy.getDecoratedMetadataFromCache(anyString(), eq(BusinessEntity.PurchaseHistory))) //
                .thenReturn(Arrays.asList( //
                        purchaseHistorySchema.get("AM_650050C066EF46905EC469E9CC2921E0__EVER__HP") //
        ));
        MatchInput input = new MatchInput();
        input.setTenant(new Tenant(DataCloudConstants.SERVICE_TENANT));
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        input.setOperationalMode(OperationalMode.CDL_LOOKUP);

        input.setPredefinedSelection(ColumnSelection.Predefined.Enrichment);
        List<ColumnMetadata> cms = cdlMetadataService.parseMetadata(input);
        assertHasColumn(cms, "Website");
        assertNotHaveColumn(cms, "AdvertisingTechnologiesTopAttributes");
        assertHasColumn(cms, "engine_mc7o9gwpq8gfw0wzkvekmw");
        assertNotHaveColumn(cms, "AM_650050C066EF46905EC469E9CC2921E0__EVER__HP");

        input.setPredefinedSelection(ColumnSelection.Predefined.TalkingPoint);
        cms = cdlMetadataService.parseMetadata(input);
        assertHasColumn(cms, "Website");
        assertHasColumn(cms, "AdvertisingTechnologiesTopAttributes");
        assertHasColumn(cms, "engine_mc7o9gwpq8gfw0wzkvekmw");
        assertHasColumn(cms, "AM_650050C066EF46905EC469E9CC2921E0__EVER__HP");

        input.setPredefinedSelection(null);
        ColumnSelection columnSelection = new ColumnSelection();
        List<Column> columns = Arrays.asList( //
                new Column("AdvertisingTechnologiesTopAttributes"), //
                new Column("City"), //
                new Column("Country") //
        );
        columnSelection.setColumns(columns);
        input.setCustomSelection(columnSelection);
        cms = cdlMetadataService.parseMetadata(input);
        assertNotHaveColumn(cms, "Website");
        assertHasColumn(cms, "AdvertisingTechnologiesTopAttributes");
        assertNotHaveColumn(cms, "Country");

        input.setCustomSelection(null);
        UnionSelection unionSelection = new UnionSelection();
        unionSelection.setPredefinedSelections(ImmutableMap.of( //
                ColumnSelection.Predefined.Enrichment, "1.0", //
                ColumnSelection.Predefined.Model, "1.0" //
        ));
        unionSelection.setCustomSelection(columnSelection);
        input.setUnionSelection(unionSelection);
        cms = cdlMetadataService.parseMetadata(input);
        assertHasColumn(cms, "AccountId");
        assertHasColumn(cms, "Website");
        assertHasColumn(cms, "AdvertisingTechnologiesTopAttributes");
        assertHasColumn(cms, "engine_mc7o9gwpq8gfw0wzkvekmw");
        assertNotHaveColumn(cms, "AM_650050C066EF46905EC469E9CC2921E0__EVER__HP");
    }

    @Test(groups = "functional")
    public void testPlan() {
        when(servingStoreProxy.getDecoratedMetadataFromCache(anyString(), eq(BusinessEntity.Account))) //
                .thenReturn(Arrays.asList( //
                        accountSchema.get("AccountId"), //
                        accountSchema.get("Website"), //
                        accountSchema.get("City"), //
                        accountSchema.get("AdvertisingTechnologiesTopAttributes") //

                ));
        when(dataCollectionProxy.getDynamoDataUnits(anyString(), any(), any()))
                .thenReturn(TestCDLMatchUtils.mockDynamoDataUnits());
        String[] fields = new String[]{ "ID", InterfaceName.AccountId.name() };
        Object[][] data = new Object[][] { { 123, ACCOUNT_ID } };
        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data, fields);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        input.setPredefinedSelection(ColumnSelection.Predefined.Enrichment);
        input.setKeyMap(ImmutableMap.of(MatchKey.LookupId, Collections.singletonList(InterfaceName.AccountId.name())));
        MatchContext context = realTimeMatchPlanner.plan(input);
        Assert.assertTrue(CollectionUtils.isNotEmpty(context.getCustomDataUnits()));
        Assert.assertTrue(CollectionUtils.isNotEmpty(context.getInternalResults()));
        InternalOutputRecord record = context.getInternalResults().get(0);
        Assert.assertEquals(record.getLookupIdKey(), InterfaceName.AccountId.name());
        Assert.assertEquals(record.getLookupIdValue(), ACCOUNT_ID);
    }

    private void assertHasColumn(List<ColumnMetadata> cms, String columnName) {
        Assert.assertNotNull(cms);
        Assert.assertTrue(cms.stream().anyMatch(cm -> columnName.equalsIgnoreCase(cm.getAttrName())),
                "Should have " + columnName + ", but got " + JsonUtils.pprint(cms));
    }

    private void assertNotHaveColumn(List<ColumnMetadata> cms, String columnName) {
        Assert.assertNotNull(cms);
        Assert.assertFalse(cms.stream().anyMatch(cm -> columnName.equalsIgnoreCase(cm.getAttrName())),
                "Should not have " + columnName + ", but got " + JsonUtils.pprint(cms));
    }

    private void loadAccountSchema() {
        List<ColumnMetadata> cms = TestCDLMatchUtils.loadAccountSchema(BusinessEntity.Account);
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
