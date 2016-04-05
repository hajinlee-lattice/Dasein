package com.latticeengines.propdata.match.service.impl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.propdata.match.testframework.PropDataMatchFunctionalTestNGBase;

@Component
public class RealTimeMatchPlannerTestNG extends PropDataMatchFunctionalTestNGBase {

    @Autowired
    RealTimeMatchPlanner matchPlanner;

    @Test(groups = "functional")
    public void testPrepareOutput() {
        MatchInput input = new MatchInput();
        input.setUuid(UUID.randomUUID());
        input.setTenant(new Tenant("PD_Test"));
        input.setPredefinedSelection(ColumnSelection.Predefined.Model);
        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.Domain, Collections.singletonList("Domain"));
        keyMap.put(MatchKey.Name, Collections.singletonList("CompanyName"));
        keyMap.put(MatchKey.City, Collections.singletonList("City"));
        keyMap.put(MatchKey.State, Collections.singletonList("State_Province"));
        keyMap.put(MatchKey.Country, Collections.singletonList("Country"));
        input.setKeyMap(keyMap);
        input.setFields(Arrays.asList("ID", "Domain", "CompanyName", "City", "State_Province", "Country"));

        List<List<Object>> mockData = MatchInputValidatorUnitTestNG.generateMockData(100);
        input.setData(mockData);

        Set<String> uniqueDomains = new HashSet<>();
        for (List<Object> row : mockData) {
            String domain = (String) row.get(1);
            Assert.assertTrue(domain.contains("abc@"));
            uniqueDomains.add(domain);
        }

        MatchContext context = matchPlanner.plan(input);
        Assert.assertEquals(context.getDomains().size(), uniqueDomains.size());

        for (InternalOutputRecord record : context.getInternalResults()) {
            Assert.assertFalse(record.isMatched());
            Assert.assertFalse(record.getParsedDomain().contains("abc@"));
        }
    }

}
