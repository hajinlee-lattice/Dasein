package com.latticeengines.datacloud.match.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;

@Component
public class RealTimeMatchPlannerTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Autowired
    @Qualifier("realTimeMatchPlanner")
    MatchPlanner matchPlanner;

    @Autowired
    @Qualifier("accountMasterColumnSelectionService")
    private ColumnSelectionService columnSelectionService;

    @Test(groups = "functional")
    public void testPrepareOutput() {
        MatchInput input = prepareMatchInput();

        Set<String> uniqueDomains = new HashSet<>();
        for (List<Object> row : input.getData()) {
            String domain = (String) row.get(1);
            Assert.assertTrue(domain.contains("abc@"));
            uniqueDomains.add(domain);
        }

        MatchContext context = matchPlanner.plan(input);
        Assert.assertEquals(context.getInput().getPredefinedVersion(),
                columnSelectionService.getCurrentVersion(Predefined.Model));
        Assert.assertEquals(context.getDomains().size(), uniqueDomains.size());

        for (InternalOutputRecord record : context.getInternalResults()) {
            Assert.assertFalse(record.isMatched());
            Assert.assertFalse(record.getParsedDomain().contains("abc@"));
        }
    }

    @Test(groups = "functional")
    public void testUnionSelection() {
        MatchInput input = prepareMatchInput();
        ColumnSelection columnSelection = new ColumnSelection();
        List<Column> columns = Arrays.asList(new Column("TechIndicator_Dropbox"), new Column("TechIndicator_Box"),
                new Column("TechIndicator_Splunk"));
        columnSelection.setColumns(columns);
        UnionSelection unionSelection = new UnionSelection();
        Map<Predefined, String> map = new HashMap<>();
        map.put(Predefined.RTS, "1.0");
        unionSelection.setPredefinedSelections(map);
        unionSelection.setCustomSelection(columnSelection);
        input.setUnionSelection(unionSelection);
        MatchContext context = matchPlanner.plan(input);
        Integer expectedColumns = columnSelectionService.parsePredefinedColumnSelection(Predefined.RTS,
                null).getColumns().size() + 3;
        Assert.assertEquals((Integer) context.getColumnSelection().getColumns().size(), expectedColumns);
    }

    private MatchInput prepareMatchInput() {
        MatchInput input = new MatchInput();
        input.setRootOperationUid(UUID.randomUUID().toString());
        input.setTenant(new Tenant("PD_Test"));
        input.setPredefinedSelection(Predefined.Model);
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.Domain, Collections.singletonList("Domain"));
        keyMap.put(MatchKey.Name, Collections.singletonList("CompanyName"));
        keyMap.put(MatchKey.City, Collections.singletonList("City"));
        keyMap.put(MatchKey.State, Collections.singletonList("State_Province"));
        keyMap.put(MatchKey.Country, Collections.singletonList("Country"));
        input.setKeyMap(keyMap);
        input.setFields(Arrays.asList("ID", "Domain", "CompanyName", "City", "State_Province", "Country", "DUNS"));

        List<List<Object>> mockData = MatchInputValidatorUnitTestNG.generateMockData(100);
        input.setData(mockData);
        return input;
    }

}
