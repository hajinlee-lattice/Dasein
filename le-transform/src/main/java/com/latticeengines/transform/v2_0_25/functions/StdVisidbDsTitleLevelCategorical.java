package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;
import com.latticeengines.transform.v2_0_25.common.DSUtils;

public class StdVisidbDsTitleLevelCategorical implements RealTimeTransform {

    public StdVisidbDsTitleLevelCategorical(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments, Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return "";

        String s = n.toString().toLowerCase();

        return getDSTitleLevelCategorical(s);
    }

    public static String getDSTitleLevelCategorical(String title) {
        if (StringUtils.isEmpty(title))
            return "Staff";

        title = title.trim().toLowerCase();

        if (DSUtils.hasUnUsualChar(title))
            return "";

        if (title.contains("vice"))
            return "Vice President";

        if (title.contains("director"))
            return "Director";

        if (title.contains("manager"))
            return "Manager";

        return "Staff";
    }
}
