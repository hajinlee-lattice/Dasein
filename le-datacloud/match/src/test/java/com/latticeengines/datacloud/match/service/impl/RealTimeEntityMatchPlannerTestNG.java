package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput.EntityKeyMap;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

public class RealTimeEntityMatchPlannerTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Inject
    private RealTimeEntityMatchPlanner matchPlanner;

    @Value("${datacloud.match.default.decision.graph.account}")
    private String defaultAccountGraph;

    @Test(groups = "functional")
    public void testMatchPlanner() {
        // Set up MatchInput.
        MatchInput input = prepareMatchInput();

        // Test changes to MatchInput.
        MatchContext context = matchPlanner.plan(input);
        Assert.assertNull(input.getDataCollectionVersion());
        Assert.assertEquals(input.getDecisionGraph(), defaultAccountGraph);
        Assert.assertEquals(input.getMatchEngine(), MatchContext.MatchEngine.REAL_TIME.getName());

        // Test MatchContext creation.
        Assert.assertEquals(context.getMatchEngine(), MatchContext.MatchEngine.REAL_TIME);
        Assert.assertTrue(context.isSeekingIdOnly());
        Assert.assertFalse(context.isCdlLookup());

        // Check Column Metadata
        List<Column> expectedColumns = new ArrayList<>();
        expectedColumns.add(new Column(InterfaceName.EntityId.name()));
        expectedColumns.add(new Column(InterfaceName.AccountId.name()));
        expectedColumns.add(new Column(InterfaceName.LatticeAccountId.name()));
        Assert.assertEquals(context.getColumnSelection().getColumns(), expectedColumns);

        // Test MatchOutput set up.
        MatchOutput output = context.getOutput();
        Assert.assertNotNull(output.getEntityKeyMaps());
        Assert.assertEquals(output.getEntityKeyMaps().size(), 1);
        Assert.assertTrue(output.getEntityKeyMaps().containsKey(BusinessEntity.Account.name()));
    }

    private MatchInput prepareMatchInput() {
        MatchInput input = new MatchInput();
        input.setRootOperationUid(UUID.randomUUID().toString());
        input.setTenant(new Tenant("PD_Test"));
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);
        input.setSkipKeyResolution(true);
        input.setFields(Arrays.asList("ID", "Domain", "CompanyName", "City", "State_Province", "Country", "DUNS",
                "CustomerAccountId", "SfdcId", "MktoId", "StreetAddress1", "StreetAddress2"));
        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        input.setAllocateId(false);
        input.setTargetEntity(BusinessEntity.Account.name());

        EntityKeyMap entityKeyMap = new EntityKeyMap();
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.Domain, Collections.singletonList("Domain"));
        keyMap.put(MatchKey.Name, Collections.singletonList("CompanyName"));
        keyMap.put(MatchKey.City, Collections.singletonList("City"));
        keyMap.put(MatchKey.State, Collections.singletonList("State_Province"));
        keyMap.put(MatchKey.Country, Collections.singletonList("Country"));
        keyMap.put(MatchKey.SystemId, Arrays.asList("CustomerAccountId", "MktoId", "SfdcId"));
        keyMap.put(MatchKey.StreetAddress1, Collections.singletonList("StreetAddress1"));
        keyMap.put(MatchKey.StreetAddress2, Collections.singletonList("StreetAddress2"));
        entityKeyMap.setKeyMap(keyMap);
        input.setEntityKeyMaps(new HashMap<>());
        input.getEntityKeyMaps().put(BusinessEntity.Account.name(), entityKeyMap);

        List<List<Object>> mockData = MatchInputValidatorUnitTestNG.generateMockData(100);
        input.setData(mockData);
        return input;
    }
}
