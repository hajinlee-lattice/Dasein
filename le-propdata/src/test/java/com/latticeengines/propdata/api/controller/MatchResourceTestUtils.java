package com.latticeengines.propdata.api.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.security.Tenant;

public class MatchResourceTestUtils {

    static MatchInput prepareSimpleMatchInput(List<List<Object>> mockData) {
        return prepareSimpleMatchInput(mockData, true);
    }

    static MatchInput prepareSimpleMatchInput(List<List<Object>> mockData, boolean resolveKeyMap) {
        MatchInput input = new MatchInput();
        input.setMatchEngine(MatchInput.MatchEngine.RealTime);
        input.setTenant(new Tenant("PD_Test"));
        List<String> fields = Arrays.asList("ID", "Domain", "Name", "City", "State", "Country");
        input.setFields(fields);
        if (resolveKeyMap) {
            input.setKeyMap(MatchKeyUtils.resolveKeyMap(fields));
        }
        input.setData(mockData);
        input.setPredefinedSelection(ColumnSelection.Predefined.Model);
        return input;
    }

    static MatchInput prepareSimpleMatchInput(Object[][] data) {
        List<List<Object>> mockData = new ArrayList<>();
        for (Object[] row : data) {
            mockData.add(Arrays.asList(row));
        }
        return prepareSimpleMatchInput(mockData);
    }

    static List<List<Object>> getGoodInputData() {
        Object[][] data = new Object[][] { { 0, "moyanne.com", "Moyanne", "Lynchburg", "Virginia", "USA" },
                { 1, "jhip.com", "Jacobson Holman PLLC", "Washington", "Washington D.C.", "USA" },
                { 2, "culturaltourismdc.org", "Captive Insurance Co", "Washington", "Washington D.C.", "USA" },
                { 3, "thehousedc.org", "House Dc", "Washington", "Washington D.C.", "USA" },
                { 4, "thetaxcenter.com", "Government Employee's Tax Specialists", "Washington", "Washington D.C.",
                        "USA" },
                { 5, "investmentwires.com", "Investmentwires Inc.", "Chagrin Falls", "Ohio", "USA" },
                { 6, "chadbentz.com", "Chad Bentz", "Chagrin Falls", "Ohio", "USA" },
                { 7, "countrysidemasonry.com", "Country Side Masonry", "Conneaut", "Ohio", "USA" },
                { 8, "hickmanlandscape.com", "Hickman Lawn Care Inc", "Columbus", "Ohio", "USA" },
                { 9, "suntool.com", "Sun Tool Co", "Las Cruces", "New Mexico", "USA" } };
        List<List<Object>> mockData = new ArrayList<>();
        for (Object[] row : data) {
            mockData.add(Arrays.asList(row));
        }
        return mockData;
    }

}
