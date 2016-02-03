package com.latticeengines.propdata.api.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.MatchInput;
import com.latticeengines.domain.exposed.propdata.manage.MatchKey;
import com.latticeengines.domain.exposed.security.Tenant;

public class MatchResourceTestUtils {

    static MatchInput prepareSimpleMatchInput(List<List<Object>> mockData) {
        MatchInput input = new MatchInput();
        input.setMatchEngine(MatchInput.MatchEngine.RealTime);
        input.setTenant(new Tenant("PD_Test"));
        input.setKeys(Arrays.asList(MatchKey.Domain, MatchKey.Name, MatchKey.City, MatchKey.State, MatchKey.Country));
        input.setData(mockData);
        return input;
    }

    static MatchInput prepareSimpleMatchInput(Object[][] data) {
        MatchInput input = new MatchInput();
        input.setMatchEngine(MatchInput.MatchEngine.RealTime);
        input.setTenant(new Tenant("PD_Test"));
        input.setKeys(Arrays.asList(MatchKey.Domain, MatchKey.Name, MatchKey.City, MatchKey.State, MatchKey.Country));
        List<List<Object>> mockData = new ArrayList<>();
        for (Object[] row : data) {
            mockData.add(Arrays.asList(row));
        }
        input.setData(mockData);
        return input;
    }

    static List<List<Object>> getGoodInputData() {
        Object[][] data = new Object[][] { { "moyanne.com", "Moyanne", "Lynchburg", "Virginia", "USA" },
                { "jhip.com", "Jacobson Holman PLLC", "Washington", "Washington D.C.", "USA" },
                { "culturaltourismdc.org", "Captive Insurance Co", "Washington", "Washington D.C.", "USA" },
                { "thehousedc.org", "House Dc", "Washington", "Washington D.C.", "USA" },
                { "thetaxcenter.com", "Government Employee's Tax Specialists", "Washington", "Washington D.C.", "USA" },
                { "investmentwires.com", "Investmentwires Inc.", "Chagrin Falls", "Ohio", "USA" },
                { "chadbentz.com", "Chad Bentz", "Chagrin Falls", "Ohio", "USA" },
                { "countrysidemasonry.com", "Country Side Masonry", "Conneaut", "Ohio", "USA" },
                { "hickmanlandscape.com", "Hickman Lawn Care Inc", "Columbus", "Ohio", "USA" },
                { "suntool.com", "Sun Tool Co", "Las Cruces", "New Mexico", "USA" } };
        List<List<Object>> mockData = new ArrayList<>();
        for (Object[] row : data) {
            mockData.add(Arrays.asList(row));
        }
        return mockData;
    }

}
