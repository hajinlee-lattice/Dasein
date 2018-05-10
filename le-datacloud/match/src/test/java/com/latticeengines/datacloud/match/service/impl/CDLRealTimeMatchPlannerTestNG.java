package com.latticeengines.datacloud.match.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

public class CDLRealTimeMatchPlannerTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String ACCOUNT_ID = "0012400001DOFs1AAH";

    @Mock
    private ZkConfigurationServiceImpl zkConfigurationService;

    @Mock
    private ServingStoreProxy servingStoreProxy;

    @Mock
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private RealTimeMatchPlanner realTimeMatchPlanner;

    @Inject
    private CDLMatchServiceImpl cdlMetadataService;

    @Inject
    private TestMatchInputService testMatchInputService;

    private Map<String, ColumnMetadata> accountSchema;

    @BeforeClass(groups = { "functional", "manual" } )
    public void setup() {
        MockitoAnnotations.initMocks(this);
        when(zkConfigurationService.isCDLTenant(any())).thenReturn(true);

        realTimeMatchPlanner.setZkConfigurationService(zkConfigurationService);
        cdlMetadataService.setServingStoreProxy(servingStoreProxy);
        cdlMetadataService.setDataCollectionProxy(dataCollectionProxy);

        loadAccountSchema();
    }

    @Test(groups = "functional")
    public void testParseMetadata() {
        when(servingStoreProxy.getDecoratedMetadataFromCache(anyString(), any(BusinessEntity.class))) //
                .thenReturn(Arrays.asList( //
                        accountSchema.get("AccountId"), //
                        accountSchema.get("Website"), //
                        accountSchema.get("City"), //
                        accountSchema.get("longitude") //

        ));
        MatchInput input = new MatchInput();
        input.setTenant(new Tenant(DataCloudConstants.SERVICE_TENANT));
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());

        input.setPredefinedSelection(ColumnSelection.Predefined.Enrichment);
        List<ColumnMetadata> cms = cdlMetadataService.parseMetadata(input);
        assertHasColumn(cms, "Website");
        assertNotHaveColumn(cms, "longitude");

        input.setPredefinedSelection(ColumnSelection.Predefined.TalkingPoint);
        cms = cdlMetadataService.parseMetadata(input);
        assertHasColumn(cms, "Website");
        assertHasColumn(cms, "longitude");

        input.setPredefinedSelection(null);
        ColumnSelection columnSelection = new ColumnSelection();
        List<Column> columns = Arrays.asList( //
                new Column("longitude"), //
                new Column("City"), //
                new Column("Country") //
        );
        columnSelection.setColumns(columns);
        input.setCustomSelection(columnSelection);
        cms = cdlMetadataService.parseMetadata(input);
        assertNotHaveColumn(cms, "Website");
        assertHasColumn(cms, "longitude");
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
        assertHasColumn(cms, "longitude");
    }

    @Test(groups = "functional")
    public void testPlan() {
        when(servingStoreProxy.getDecoratedMetadataFromCache(anyString(), any(BusinessEntity.class))) //
                .thenReturn(Arrays.asList( //
                        accountSchema.get("AccountId"), //
                        accountSchema.get("Website"), //
                        accountSchema.get("City"), //
                        accountSchema.get("longitude") //

                ));
        when(dataCollectionProxy.getAccountDynamo(anyString(), any())).thenReturn(TestCDLMatchUtils.mockCustomAccount());
        String[] fields = new String[]{ "ID", InterfaceName.AccountId.name() };
        Object[][] data = new Object[][] { { 123, ACCOUNT_ID } };
        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data, fields);
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        input.setPredefinedSelection(ColumnSelection.Predefined.Enrichment);
        input.setKeyMap(ImmutableMap.of(MatchKey.LookupId, Collections.singletonList(InterfaceName.AccountId.name())));
        MatchContext context = realTimeMatchPlanner.plan(input);
        Assert.assertNotNull(context.getCustomAccountDataUnit());
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
        List<ColumnMetadata> cms = TestCDLMatchUtils.loadAccountSchema();
        accountSchema = new HashMap<>();
        cms.forEach(cm -> accountSchema.put(cm.getAttrName(), cm));
    }

}
