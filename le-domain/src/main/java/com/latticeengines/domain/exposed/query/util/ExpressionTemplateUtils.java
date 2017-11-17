package com.latticeengines.domain.exposed.query.util;

import com.google.common.annotations.VisibleForTesting;

public class ExpressionTemplateUtils {

    private static String currentDate = "GETDATE()";

    @VisibleForTesting
    public static void setCurrentDate(String currentDate) {
        ExpressionTemplateUtils.currentDate = currentDate;
    }

    public static String strAttrToDate(String attrName) {
        return String.format("TO_DATE(%s, %s)", attrName, "'YYYY-MM-DD'");
    }

    public static String getDateOnPeriodTemplate(String p, String path) {
        return String.format("DATE_TRUNC('%s', %s)", p, path);
    }

    public static String getDateTargetValueOnPeriodTemplate(String p, int unit, String baseDate) {
        return String.format("DATEADD('%s', %d, %s)", p, unit, getDateOnPeriodTemplate(p, baseDate));
    }

    public static String getCurrentDate() {
        return currentDate;
    }
}
