package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsPdAlexaRelatedlinksCount implements RealTimeTransform {

    public StdVisidbDsPdAlexaRelatedlinksCount(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments,
            Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return null;

        String s = n.toString().trim().toLowerCase();

        return calculateStdVisidbDsPdAlexaRelatedlinksCount(s);
    }

    public static Integer calculateStdVisidbDsPdAlexaRelatedlinksCount(
            String alexaRelatedLinks) {
        if (StringUtils.isEmpty(alexaRelatedLinks))
            return null;

        return StringUtils.countMatches(alexaRelatedLinks, ",") + 1;
    }
}
