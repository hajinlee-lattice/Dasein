package com.latticeengines.datacloud.match.testframework;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;

public class TestMatchInputUtils {

    public static MatchInput prepareSimpleMatchInput(List<List<Object>> mockData, boolean resolveKeyMap) {
        List<String> inputFields = Arrays.asList("ID", "Domain", "Name", "City", "State", "Country");
        return prepareSimpleMatchInput(mockData, inputFields, resolveKeyMap);
    }

    public static MatchInput prepareSimpleMatchInput(List<List<Object>> mockData, List<String> inputFields, boolean resolveKeyMap) {
        MatchInput input = new MatchInput();
        input.setPredefinedSelection(Predefined.RTS);
        input.setTenant(new Tenant(DataCloudConstants.SERVICE_TENANT));
        input.setFields(inputFields);
        if (resolveKeyMap) {
            input.setKeyMap(MatchKeyUtils.resolveKeyMap(inputFields));
        }
        input.setData(mockData);
        return input;
    }

    public static MatchInput prepareSimpleMatchInput(Object[][] data) {
        return prepareSimpleMatchInput(data, null);
    }

    public static MatchInput prepareSimpleMatchInput(Object[][] data, String[] fields) {
        List<List<Object>> mockData = new ArrayList<>();
        for (Object[] row : data) {
            mockData.add(Arrays.asList(row));
        }
        if (fields == null || fields.length == 0) {
            return prepareSimpleMatchInput(mockData, true);
        } else {
            return prepareSimpleMatchInput(mockData, Arrays.asList(fields), true);
        }
    }

}
