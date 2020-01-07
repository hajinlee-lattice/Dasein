package com.latticeengines.domain.exposed.cdl.activity;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.latticeengines.common.exposed.util.TemplateUtils;
import com.latticeengines.domain.exposed.StringTemplateConstants;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.TimeFilter;

public class ActivityMetricsGroupUtils {
    private static final int GROUPID_UPPER_BOUND = 6;
    private static final int GROUPID_LOWER_BOUND = 3;
    private static final Pattern ATTR_NAME_PATTERN = //
            Pattern.compile("am_(?<groupId>[^_]+)__(?<rollupDims>.*)__(?<timeRange>.*)");
    private static final Pattern VALID_GROUPID_CHAR = Pattern.compile("[0-9a-zA-Z]+");
    private static final Character FILL_CHAR = 'x';
    private static final BiMap<String, String> RELATION_STR = new ImmutableBiMap.Builder<String, String>() //
            .put(ComparisonType.WITHIN.toString(), "w") //
            .put(ComparisonType.BETWEEN.toString(), "b") //
            .build();
    private static final BiMap<String, String> PERIOD_STR = new ImmutableBiMap.Builder<String, String>() //
            .put(PeriodStrategy.Template.Week.toString(), "w").build();

    // (operator in timeRange tmpl) <--> (description)
    private static final BiMap<String, String> RELATION_DESCRIPTION = new ImmutableBiMap.Builder<String, String>()
            .put("w", "in last") // within
            .put("b", "between") // between
            .build();


    // generate groupId from groupName
    public static String fromGroupNameToGroupIdBase(String groupName) {
        String[] gnWords = groupName.split("\\s+");
        StringBuilder groupId = new StringBuilder();
        for (int i = 0; i < GROUPID_UPPER_BOUND && i < gnWords.length; i++) {
            Character c = getFirstValidChar(gnWords[i]);
            if (c != null) {
                groupId.append(c);
            }
        }
        while (groupId.length() < GROUPID_LOWER_BOUND) {
            groupId.append(FILL_CHAR);
        }
        return groupId.toString().toLowerCase();
    }

    // split into groupId, rollupDims, timeRange
    public static List<String> parseAttrName(String attrName) throws ParseException {
        if (StringUtils.isBlank(attrName)) {
            throw new ParseException("Cannot parse empty attribute name.", 0);
        }
        Matcher matcher = ATTR_NAME_PATTERN.matcher(attrName);
        if (matcher.matches()) {
            return Arrays.asList( //
                    matcher.group("groupId"), //
                    matcher.group("rollupDims"), //
                    matcher.group("timeRange") //
            );
        } else {
            throw new ParseException("Cannot parse attribute name " + attrName, 0);
        }
    }

    public static String timeFilterToTimeRangeTmpl(TimeFilter timeFilter) {
        Map<String, Object> map = new HashMap<>();
        map.put("operator", getValueFromBiMap(RELATION_STR, timeFilter.getRelation().toString()));
        map.put("period", getValueFromBiMap(PERIOD_STR, timeFilter.getPeriod()));
        map.put("params", timeFilter.getValues());
        return TemplateUtils.renderByMap(StringTemplateConstants.ACTIVITY_METRICS_GROUP_TIME_RANGE, map);
    }

    public static TimeFilter timeRangeTmplToTimeFilter(String timeRange) {
        String[] fragments = timeRange.split("_");
        String op = fragments[0];
        switch (op) {
            case "w":
                try {
                    return parseSingleVars(fragments);
                } catch (ParseException e) {
                    throw new IllegalArgumentException("Cannot parse " + timeRange);
                }
            case "b":
                try {
                    return parseTwoVals(fragments);
                } catch (ParseException e) {
                    throw new IllegalArgumentException("Cannot parse " + timeRange);
                }
            default:
                throw new IllegalArgumentException("Unknown operator " + op);
        }
    }

    private static TimeFilter parseSingleVars(String[] fragments) throws ParseException {
        if (fragments.length != 3) {
            throw new ParseException("Wrong number of fragments.", 0);
        }
        ComparisonType op = ComparisonType.getByName((String) getValueFromBiMap(RELATION_STR.inverse(), fragments[0].toLowerCase()));
        String period = (String) getValueFromBiMap(PERIOD_STR.inverse(), fragments[fragments.length - 1]);
        List<Object> vals = Collections.singletonList(Long.parseLong(fragments[1]));
        return new TimeFilter(op, period, vals);
    }

    private static TimeFilter parseTwoVals(String[] fragments) throws ParseException {
        if (fragments.length != 4) {
            throw new ParseException("Wrong number of fragments.", 0);
        }
        ComparisonType op = ComparisonType.getByName((String) getValueFromBiMap(RELATION_STR.inverse(), fragments[0].toLowerCase()));
        String period = (String) getValueFromBiMap(PERIOD_STR.inverse(), fragments[fragments.length - 1]);
        List<Object> vals = Arrays.asList(Long.parseLong(fragments[1]), Long.parseLong(fragments[2]));
        return new TimeFilter(op, period, vals);
    }

    public static String timeRangeTmplToDescription(String timeRange) {
        String op = timeRange.substring(0, timeRange.indexOf("_"));
        String descTemplate;
        switch (op) {
            case "w":
                descTemplate = StringTemplateConstants.SINGLE_VAL_TIME_RANGE_DESC;
                break;
            case "b":
                descTemplate = StringTemplateConstants.DOUBLE_VAL_TIME_RANGE_DESC;
                break;
            default:
                throw new IllegalArgumentException("Unknown operator " + op);
        }
        String[] fragments = timeRange.split("_");
        String operatorLetter = fragments[0].substring(0, 1);
        String periodLetter = fragments[fragments.length - 1].substring(0, 1);
        Map<String, Object> map = new HashMap<>();
        map.put("operator", getValueFromBiMap(RELATION_DESCRIPTION, operatorLetter).toString().toLowerCase());
        map.put("period", getValueFromBiMap(PERIOD_STR.inverse(), periodLetter).toString().toLowerCase());
        map.put("params", ArrayUtils.subarray(fragments, 1, fragments.length - 1));
        return TemplateUtils.renderByMap(descTemplate, map);
    }

    private static Object getValueFromBiMap(BiMap<?, ?> map, Object key) {
        Object value = map.get(key);
        if (value == null) {
            throw new IllegalArgumentException(String.format("%s is not valid.", key));
        }
        return value;
    }

    private static Character getFirstValidChar(String word) {
        Matcher matcher = VALID_GROUPID_CHAR.matcher(word);
        if (matcher.find()) {
            return word.charAt(matcher.start());
        }
        return null;
    }
}
