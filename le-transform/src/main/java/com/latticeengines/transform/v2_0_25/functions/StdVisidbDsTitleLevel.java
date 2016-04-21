package com.latticeengines.transform.v2_0_25.functions;

import java.util.Map;
import java.util.regex.Pattern;

import com.latticeengines.transform.exposed.RealTimeTransform;

public class StdVisidbDsTitleLevel implements RealTimeTransform {

    public StdVisidbDsTitleLevel(String modelPath) {
    }

    public Object transform(Map<String, Object> arguments,
            Map<String, Object> record) {
        String column = (String) arguments.get("column");
        Object n = record.get(column);

        if (n == null)
            return 0;

        String s = n.toString().toLowerCase();

        return calculateStdVisidbDsTitleLevel(s);
    }

    public static int calculateStdVisidbDsTitleLevel(String s) {
        return StdVisidbDsTitleLevel.dsTitleIssenior(s)
                + (2 * StdVisidbDsTitleLevel.dsTitleIsmanager(s) + (4 * StdVisidbDsTitleLevel
                        .dsTitleIsdirector(s) + 8 * StdVisidbDsTitleLevel
                        .dsTitleIsvpabove(s)));
    }

    private static int dsTitleIssenior(String s) {
        return Pattern.matches("(.*?\\b)sr(\\b.*?)|(.*?\\b)senior(\\b.*?)", s) ? 1
                : 0;
    }

    private static int dsTitleIsmanager(String s) {
        return Pattern.matches("(.*?\\b)mgr(.*)|(.*)manag(.*)", s) ? 1 : 0;
    }

    private static int dsTitleIsdirector(String s) {
        return Pattern.matches("(.*?\\b)dir(.*)", s) ? 1 : 0;
    }

    private static int dsTitleIsvpabove(String s) {
        return Pattern
                .matches(
                        "(.*?\\b)vp(\\b.*?)|(.*?\\b)pres(.*)|(.*?\\b)chief(\\b.*?)|(.*)(?<!\\w)c[tefmxdosi]o(?!\\w)(.*)",
                        s) ? 1 : 0;
    }
}
