package com.latticeengines.domain.exposed;

public final class StringTemplateConstants {

    protected StringTemplateConstants() {
        throw new UnsupportedOperationException();
    }

    // Template tokens/values
    public static final String ACTIVITY_METRICS_GROUP_TIME_RANGE_TOKEN = "TimeRange";
    public static final String ACTIVITY_METRICS_GROUP_TIME_RANGE = "${operator}_${params?join(\"_\")}_${period}";
    public static final String ACTIVITY_METRICS_GROUP_TIME_RANGE_NO_VAL = "${operator}_${period}";
    public static final String SINGLE_VAL_TIME_RANGE_DESC = "${operator} ${params?join(\"_\")} ${period}";
    public static final String DOUBLE_VAL_TIME_RANGE_DESC = "${operator} ${params?join(\" and \")} ${period}";
    public static final String ACTIVITY_METRICS_GROUP_ATTRNAME = "am_${GroupId}__${RollupDimIds?join(\"_\")}__${" + ACTIVITY_METRICS_GROUP_TIME_RANGE_TOKEN + "}";
    public static final String ACTIVITY_METRICS_ATTR_SECONDARY_DISPLAYNAME = "(%s to %s)";

    // Names in DB
    public static final String ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DISPLAYNAME = "ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DISPLAYNAME";
    public static final String ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DESCRIPTION = "ACTIVITY_METRICS_GROUP_TOTAL_VISIT_DESCRIPTION";
    public static final String ACTIVITY_METRICS_GROUP_SUBCATEGORY = "ACTIVITY_METRICS_GROUP_SUBCATEGORY";
    public static final String ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DISPLAYNAME = "ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DISPLAYNAME" ;
    public static final String ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DESCRIPTION = "ACTIVITY_METRICS_GROUP_SOURCEMEDIUM_DESCRIPTION";
    public static final String ACTIVITY_METRICS_GROUP_SECONDARY_SUBCATEGORY = "ACTIVITY_METRICS_GROUP_SECONDARY_SUBCATEGORY";

    public static final String OPPORTUNITY_METRICS_GROUP_STAGENAME_DISPLAYNAME = "OPPORTUNITY_METRICS_GROUP_STAGENAME_DISPLAYNAME";
    public static final String OPPORTUNITY_METRICS_GROUP_STAGENAME_DESCRIPTION =
            "OPPORTUNITY_METRICS_GROUP_STAGENAME_DESCRIPTION";
    public static final String OPPORTUNITY_METRICS_GROUP_SUBCATEGORY = "OPPORTUNITY_METRICS_GROUP_SUBCATEGORY";
}
