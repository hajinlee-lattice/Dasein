package com.latticeengines.domain.exposed;

public class StringTemplates {

    public static final String ACTIVITY_METRICS_GROUP_TIME_RANGE_TOKEN = "TimeRange";

    public static final String ACTIVITY_METRICS_GROUP_TIME_RANGE = "${operator}_${params?join(\"_\")}_${period}"; // e.g. l_${params}_w for last <param> week(s)

    public static final String ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DISPLAYNAME = "Visited ${" + ACTIVITY_METRICS_GROUP_TIME_RANGE_TOKEN + "}";

    public static final String ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DESCRIPTION = "Accounts visited ${PathPatternId.PathPatternName} ${" + ACTIVITY_METRICS_GROUP_TIME_RANGE_TOKEN + "}";

    public static final String ACTIVITY_METRICS_GROUP_SUBCATEGORY = "${PathPatternId.PathPatternName}";

    public static final String ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DISPLAYNAME = "Visited from <#if SourceMediumId.SourceMedium == '__others__'>all other sources<#else>${SourceMediumId.SourceMedium}</#if> ${" + ACTIVITY_METRICS_GROUP_TIME_RANGE_TOKEN + "}" ;

    public static final String ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DESCRIPTION = "Accounts visited ${PathPatternId.PathPatternName} from <#if SourceMediumId.SourceMedium == '__others__'>all other sources<#else>${SourceMediumId.SourceMedium}</#if> ${" + ACTIVITY_METRICS_GROUP_TIME_RANGE_TOKEN + "}";

    public static final String ACTIVITY_METRICS_GROUP_ATTRNAME = "am_${GroupId}__${RollupDimIds?join(\"_\")}__${" + ACTIVITY_METRICS_GROUP_TIME_RANGE_TOKEN + "}";
}
