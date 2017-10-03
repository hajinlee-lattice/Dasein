package com.latticeengines.query.util;

import com.latticeengines.domain.exposed.query.TimeFilter.Period;

public class ExpressionTemplateUtils {

    public static String strAttrToDate(String attrName) {
        return String.format("TO_DATE(%s, %s)", attrName, "'YYYY-MM-DD'");
    }

    public static String getDateTemplateTruncatedOnPeriod(Period p, String path) {
        return String.format("DATE_TRUNC('%s', %s)", p.name(), path);
    }

    public static String getDateTemplateValueOnPeriod(Period p, int unit, String baseDate) {
        return String.format("DATEADD('%s', %d, %s)", p.name(), unit, getDateTemplateTruncatedOnPeriod(p, baseDate));
    }

    public static String getCurrentDate() {
        return "GETDATE()";
    }
}
