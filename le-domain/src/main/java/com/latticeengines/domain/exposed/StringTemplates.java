package com.latticeengines.domain.exposed;

public class StringTemplates {

    public static final String ACTIVITY_METRICS_GROUP_TIME_RANGE = "${operator}_${params?join(\"_\")}_${period}"; // e.g. l_${params}_w for last <param> week(s)

    public static final String ACTIVITY_METRICS_GROUP_TIME_RANGE_DESCRIPTION = "in ${operator} ${params?join(\"_\")} ${period}"; // e.g. last <param> week(s) for l_${params}_w

    public static final String ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DISPLAYNAME = "Visits ${TimeRange}";

    public static final String ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DESCRIPTION = "Number of visits to the url pattern ${PathPattern.PatternName} ${TimeRange}";

    public static final String ACTIVITY_METRICS_GROUP_SUBCATEGORY = "${PathPattern.PatternName}";

    public static final String ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DISPLAYNAME = "Visits ${TimeRange} from ${SourceMedium.SourceMedium}";

    public static final String ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DESCRIPTION = "Number of visits from ${SourceMedium.SourceMedium} to the url pattern ${PathPattern.PatternName} ${TimeRange}";

    public static final String ACTIVITY_METRICS_GROUP_ATTRNAME = "am_${GroupId}__${RollupDimIds?join(\"_\")}__${TimeRange}";
}
