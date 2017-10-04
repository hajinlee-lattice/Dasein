package com.latticeengines.domain.exposed.query.util;

import com.latticeengines.domain.exposed.query.TimeFilter.Period;

public class ExpressionTemplateUtils {

    public static String strAttrToDate(String attrName) {
        return String.format("TO_DATE(%s, %s)", attrName, "'YYYY-MM-DD'");
    }

    public static String getDateOnPeriodTemplate(Period p, String path) {
        return String.format("DATE_TRUNC('%s', %s)", p.name(), path);
    }

    public static String getDateTargetValueOnPeriodTemplate(Period p, int unit, String baseDate) {
        return String.format("DATEADD('%s', %d, %s)", p.name(), unit, getDateOnPeriodTemplate(p, baseDate));
    }

    public static String getCurrentDate() {
        return "GETDATE()";
    }

    public static String getDateDiffTemplate(Period p, String source, String target) {
        return String.format("DATEDIFF('%s', %s, %s)", p.name(), getDateOnPeriodTemplate(p, source),
                getDateOnPeriodTemplate(p, target));
    }
}
