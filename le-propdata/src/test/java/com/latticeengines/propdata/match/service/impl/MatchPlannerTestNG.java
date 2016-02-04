package com.latticeengines.propdata.match.service.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.propdata.match.testframework.PropDataMatchFunctionalTestNGBase;

@Component
public class MatchPlannerTestNG extends PropDataMatchFunctionalTestNGBase {

    @Autowired
    MatchPlanner matchPlanner;

    @Test(groups = "functional")
    public void testPrepareOutput() {
        MatchInput input = new MatchInput();
        input.setTenant(new Tenant("PD_Test"));
        input.setMatchEngine(MatchInput.MatchEngine.RealTime);
        input.setPredefinedSelection(ColumnSelection.Predefined.Model);
        Map<MatchKey, String> keyMap = new HashMap<>();
        keyMap.put(MatchKey.Domain, "Domain");
        keyMap.put(MatchKey.Name, "CompanyName");
        keyMap.put(MatchKey.City, "City");
        keyMap.put(MatchKey.State, "State_Province");
        keyMap.put(MatchKey.Country, "Country");
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
        Assert.assertEquals(context.getStatus(), MatchStatus.NEW);
        Assert.assertEquals(context.getDomains().size(), uniqueDomains.size());

        for (InternalOutputRecord record : context.getInternalResults()) {
            Assert.assertFalse(record.isMatched());
            Assert.assertFalse(record.getParsedDomain().contains("abc@"));
        }
    }

}
