package com.latticeengines.datacloud.match.service.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

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
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.datacloud.core.service.impl.ZkConfigurationServiceImpl;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestCDLMatchUtils;
import com.latticeengines.datacloud.match.testframework.TestMatchInputService;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrState;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;

public class CDLRealTimeMatchServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String ACCOUNT_ID = "0012400001DOFs1AAH";

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
    private TestMatchInputService testMatchInputService;

    @Inject
    private CDLMatchServiceImpl cdlColumnSelectionService;

    private Map<String, ColumnMetadata> accountSchema;

    @BeforeClass(groups = "functional")
    public void setup() {
        loadAccountSchema();

        MockitoAnnotations.initMocks(this);
        when(zkConfigurationService.isCDLTenant(any())).thenReturn(true);
        when(servingStoreProxy.getDecoratedMetadataFromCache(anyString(), any(BusinessEntity.class))) //
                .thenReturn(Stream.of(
                        accountSchema.get("Website"), //
                        accountSchema.get("AlexaRank"), //
                        accountSchema.get("BmbrSurge_HumanResourceManagement_Intent")
                ).peek(cm -> cm.setAttrState(AttrState.Active)).collect(Collectors.toList()));
        when(dataCollectionProxy.getAccountDynamo(anyString(), any())).thenReturn(TestCDLMatchUtils.mockCustomAccount());


        realTimeMatchPlanner.setZkConfigurationService(zkConfigurationService);
        cdlColumnSelectionService.setServingStoreProxy(servingStoreProxy);
        cdlColumnSelectionService.setDataCollectionProxy(dataCollectionProxy);
    }

    @Test(groups = "functional")
    public void testCDLLookupByAccountId() {
        Object[][] data = new Object[][] { { 123, ACCOUNT_ID } };
        MatchInput input = prepareMatchInput(data);
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertEquals(output.getResult().size(), 1);
        OutputRecord record = output.getResult().get(0);
        Assert.assertTrue(record.isMatched());
        Assert.assertNotNull(record.getOutput());
        Assert.assertNotNull(record.getOutput().get(0));
    }

    @Test(groups = "functional")
    public void testCDLLookupByAccountIdNoMatch() {
        Object[][] data = new Object[][] { { 123, "12345" } };
        MatchInput input = prepareMatchInput(data);
        MatchOutput output = realTimeMatchService.match(input);
        Assert.assertEquals(output.getResult().size(), 1);
        OutputRecord record = output.getResult().get(0);
        Assert.assertFalse(record.isMatched());
    }

    private MatchInput prepareMatchInput(Object[][] data) {
        String[] fields = new String[]{ "ID", InterfaceName.AccountId.name() };
        MatchInput input = testMatchInputService.prepareSimpleAMMatchInput(data, fields);
        input.setKeyMap(ImmutableMap.of(MatchKey.LookupId, Collections.singletonList("AccountId")));

        ColumnSelection columnSelection = new ColumnSelection();
        List<Column> columns = Arrays.asList( //
                new Column("Website"), //
                new Column("AlexaRank"), //
                new Column("BmbrSurge_HumanResourceManagement_Intent") //
        );
        columnSelection.setColumns(columns);
        input.setPredefinedSelection(null);
        input.setCustomSelection(columnSelection);
        return input;
    }

    private void loadAccountSchema() {
        List<ColumnMetadata> cms = TestCDLMatchUtils.loadAccountSchema();
        accountSchema = new HashMap<>();
        cms.forEach(cm -> accountSchema.put(cm.getAttrName(), cm));
    }

}
