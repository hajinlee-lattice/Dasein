package com.latticeengines.datacloud.match.service.impl;

import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class RealTimeEntityMatchPlannerTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Inject
    private RealTimeEntityMatchPlanner matchPlanner;

    @Value("${datacloud.match.default.decision.graph}")
    private String defaultGraph;

    @Test(groups = "functional")
    public void testMatchPlanner() {
        // Set up MatchInput.
        MatchInput input = prepareMatchInput();

        // Test changes to MatchInput.
        MatchContext context = matchPlanner.plan(input);
        Assert.assertEquals(input.getDataCollectionVersion(), null);
        Assert.assertEquals(input.getDecisionGraph(), defaultGraph);
        Assert.assertEquals(input.getMatchEngine(), MatchContext.MatchEngine.REAL_TIME.getName());

        // Test MatchContext creation.
        Assert.assertEquals(context.getMatchEngine(), MatchContext.MatchEngine.REAL_TIME);
        Assert.assertTrue(context.isSeekingIdOnly());
        Assert.assertFalse(context.isCdlLookup());

        // Check Column Metadata
        List<Column> expectedColumns = new ArrayList<>();
        expectedColumns.add(new Column(InterfaceName.EntityId.name()));
        Assert.assertEquals(context.getColumnSelection().getColumns(), expectedColumns);

        // Test MatchOutput set up.
        MatchOutput output = context.getOutput();
        Assert.assertTrue(output.getEntityKeyMap() != null);
        Assert.assertEquals(output.getEntityKeyMap().size(), 1);
        MatchInput.EntityKeyMap entityKeyMap = output.getEntityKeyMap().get(0);
        Assert.assertEquals(entityKeyMap.getBusinessEntity(), BusinessEntity.Account.name());
        Assert.assertEquals(entityKeyMap.getSystemIdPriority(), Arrays.asList("AccountId", "MktoId", "SfdcId"));
    }

    private MatchInput prepareMatchInput() {
        MatchInput input = new MatchInput();
        input.setRootOperationUid(UUID.randomUUID().toString());
        input.setTenant(new Tenant("PD_Test"));
        input.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);
        input.setSkipKeyResolution(true);
        input.setFields(Arrays.asList("ID", "Domain", "CompanyName", "City", "State_Province", "Country", "DUNS",
                "AccountId", "SfdcId", "MktoId"));
        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        input.setAllocateId(false);
        input.setTargetEntity(BusinessEntity.Account.name());
        input.setEntityKeyMapList(new ArrayList<>());

        MatchInput.EntityKeyMap entityKeyMap = new MatchInput.EntityKeyMap();
        entityKeyMap.setBusinessEntity(BusinessEntity.Account.name());
        entityKeyMap.setSystemIdPriority(Arrays.asList("AccountId", "MktoId", "SfdcId"));
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.Domain, Collections.singletonList("Domain"));
        keyMap.put(MatchKey.Name, Collections.singletonList("CompanyName"));
        keyMap.put(MatchKey.City, Collections.singletonList("City"));
        keyMap.put(MatchKey.State, Collections.singletonList("State_Province"));
        keyMap.put(MatchKey.Country, Collections.singletonList("Country"));
        keyMap.put(MatchKey.SystemId, Arrays.asList("AccountId", "MktoId", "SfdcId"));
        entityKeyMap.setKeyMap(keyMap);
        input.getEntityKeyMapList().add(entityKeyMap);

        List<List<Object>> mockData = MatchInputValidatorUnitTestNG.generateMockData(100);
        input.setData(mockData);
        return input;
    }
}
