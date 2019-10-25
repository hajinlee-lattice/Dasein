package com.latticeengines.domain.exposed.cdl.activity;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;

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
    private static final Pattern VALID_GROUPID_CHAR = Pattern.compile("[0-9a-zA-Z]+");
    private static final Character FILL_CHAR = 'x';
    private static final BiMap<String, Character> STRING_LETTER_RELATION = new ImmutableBiMap.Builder<String, Character>()
            .put(ComparisonType.LAST.toString(), 'l').put(PeriodStrategy.Template.Week.toString(), 'w').build();

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

    public static String timeFilterToTimeRangeInGroupId(TimeFilter timeFilter) {
        Map<String, Object> map = new HashMap<>();
        map.put("operator", getValueFromBiMap(STRING_LETTER_RELATION, timeFilter.getRelation().toString()));
        map.put("period", getValueFromBiMap(STRING_LETTER_RELATION, timeFilter.getPeriod()));
        map.put("params", timeFilter.getValues());
        return TemplateUtils.renderByMap(StringTemplates.ACTIVITY_METRICS_GROUP_TIME_RANGE, map);
    }

    public static String timeRangeInGroupIdToDescription(String timeRange) {
        String[] fragments = timeRange.split("_");
        Character operatorLetter = fragments[0].charAt(0);
        Character periodLetter = fragments[fragments.length - 1].charAt(0);
        Map<String, Object> map = new HashMap<>();
        map.put("operator",
                getValueFromBiMap(STRING_LETTER_RELATION.inverse(), operatorLetter).toString().toLowerCase());
        map.put("period", getValueFromBiMap(STRING_LETTER_RELATION.inverse(), periodLetter).toString().toLowerCase());
        map.put("params", ArrayUtils.subarray(fragments, 1, fragments.length - 1));
        return TemplateUtils.renderByMap(StringTemplates.ACTIVITY_METRICS_GROUP_TIME_RANGE_DESCRIPTION, map);
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
