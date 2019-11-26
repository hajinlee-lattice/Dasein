package com.latticeengines.domain.exposed.cdl.activity;

import java.text.ParseException;
import java.util.Arrays;
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
import com.latticeengines.domain.exposed.StringTemplates;
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
    private static final BiMap<String, Character> RELATION_LETTER = new ImmutableBiMap.Builder<String, Character>() //
            .put(ComparisonType.WITHIN.toString(), 'w') //
            .put(ComparisonType.BETWEEN.toString(), 'b') //
            .build();
    private static final BiMap<String, Character> PERIOD_LETTER = new ImmutableBiMap.Builder<String, Character>() //
            .put(PeriodStrategy.Template.Week.toString(), 'w').build();

    // (operator in timeRange tmpl) <--> (description)
    private static final BiMap<Character, String> RELATION_DESCRIPTION = new ImmutableBiMap.Builder<Character, String>()
            .put('w', "in last") // within
            .put('b', "between") // between
            .build();

    private static final String SINGLE_VAL_TIME_RANGE_DESC = "${operator} ${params?join(\"_\")} ${period}";
    private static final String DOUBLE_VAL_TIME_RANGE_DESC = "${operator} ${params?join(\" and \")} ${period}";


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
        return groupId.toString();
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

    public static String timeFilterToTimeRangeTemplate(TimeFilter timeFilter) {
        Map<String, Object> map = new HashMap<>();
        map.put("operator", getValueFromBiMap(RELATION_LETTER, timeFilter.getRelation().toString()));
        map.put("period", getValueFromBiMap(PERIOD_LETTER, timeFilter.getPeriod()));
        map.put("params", timeFilter.getValues());
        return TemplateUtils.renderByMap(StringTemplates.ACTIVITY_METRICS_GROUP_TIME_RANGE, map);
    }

    public static String timeRangeTmplToDescription(String timeRange) {
        String op = timeRange.substring(0, timeRange.indexOf("_"));
        String descTemplate;
        switch (op) {
            case "w":
                descTemplate = SINGLE_VAL_TIME_RANGE_DESC;
            case "b":
                descTemplate = DOUBLE_VAL_TIME_RANGE_DESC;
                break;
            default:
                throw new IllegalArgumentException("Unknown operator " + op);
        }
        String[] fragments = timeRange.split("_");
        Character operatorLetter = fragments[0].charAt(0);
        Character periodLetter = fragments[fragments.length - 1].charAt(0);
        Map<String, Object> map = new HashMap<>();
        map.put("operator", getValueFromBiMap(RELATION_DESCRIPTION, operatorLetter).toString().toLowerCase());
        map.put("period", getValueFromBiMap(PERIOD_LETTER.inverse(), periodLetter).toString().toLowerCase());
        map.put("params", ArrayUtils.subarray(fragments, 1, fragments.length - 1));
        return TemplateUtils.renderByMap(descTemplate, map);
    }

    public static String generateSourceMediumDisplayName(String timeRange, Map<String, Object> sourceMedium) {
        Map<String, Object> map = new HashMap<>();
        map.put("SourceMedium", sourceMedium);
        map.put("TimeRange", timeRange);
        return TemplateUtils.renderByMap(StringTemplates.ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DISPLAYNAME, map);
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
