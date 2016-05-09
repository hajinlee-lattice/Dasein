package com.latticeengines.transform.v2_0_25.functions;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsStateIsInRockyMountains implements RealTimeTransform {

    private static final long serialVersionUID = -1428998608448491827L;
    static HashSet<String> valueMap = new HashSet<String>(Arrays.asList("MT", "CO", "ID", "WY", "UT"));

    public StdVisidbDsStateIsInRockyMountains(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return false;

        String s = n.toString().trim().toUpperCase();

        return getDSStateIsInRockyMountain(s);
    }

    public static Boolean getDSStateIsInRockyMountain(String state) {
        if (StringUtils.isEmpty(state))
            return false;

        return valueMap.contains(state);
    }

    @Override
    public Attribute getMetadata() {
        // TODO Auto-generated method stub
        return null;
    }
}
