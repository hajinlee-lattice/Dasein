package com.latticeengines.query.factory.sqlquery;

/**
 * DateTimeExpression has different Syntax for different target systems
 * This Interface will provide the environment abstraction between target systems
 *
 * @author jadusumalli
 */
public interface DateTimeExpressionTemplate{

    default String strAttrToDate(String attrName) {
        return String.format("TO_DATE(%s, %s)", attrName, "'yyyy-MM-dd'");
    }

    default String getDateOnPeriodTemplate(String datepart, String timestamp) {
        return String.format("DATE_TRUNC('%s', %s)", datepart, timestamp);
    }

    String getDateTargetValueOnPeriodTemplate(String datepart, int unit, String timestamp);

    String getCurrentDate();

}
