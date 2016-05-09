package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import org.apache.commons.collections.OrderedMap;
import org.apache.commons.collections.map.LinkedMap;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.v2_0_25.common.DSUtils;

public class StdVisidbDsTitleScope implements RealTimeTransform {

    private static OrderedMap mapTitleScope = null;

    public StdVisidbDsTitleScope(String modelPath) {

    }

    @SuppressWarnings("unchecked")
    @Override
    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object o = record.get(column);

        if (o == null)
            return "Null";

        if (mapTitleScope == null) {
            mapTitleScope = new LinkedMap();
            mapTitleScope.put("Continental", "north america,emea,asia,africa,europ,south america");
            mapTitleScope.put("Global", "global,internaltional,worldwide");
            mapTitleScope.put("National", "us,national");
            mapTitleScope.put("Regional", "region,branch,territory,district,central,western,eastern,northern,southern");
        }

        String s = (String) o;

        return DSUtils.valueReturn(s, mapTitleScope);
    }

}
