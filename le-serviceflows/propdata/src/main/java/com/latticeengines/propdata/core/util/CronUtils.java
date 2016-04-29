package com.latticeengines.propdata.core.util;

import java.text.ParseException;
import java.util.Date;

import org.quartz.CronExpression;

public final class CronUtils {

    public static Date getPreviousFireTime(String cron) throws ParseException {
        CronExpression expression = new CronExpression(cron);
        return expression.getTimeBefore(new Date());
    }

    public static Date getNextFireTime(String cron) throws ParseException {
        CronExpression expression = new CronExpression(cron);
        return expression.getNextValidTimeAfter(new Date());
    }

    public static Date getPreviouFireTimeFromNext(String cron) throws ParseException {
        CronExpression expression = new CronExpression(cron);
        Date nextValidTime = expression.getNextValidTimeAfter(new Date());
        Date subsequentNextValidTime = expression.getNextValidTimeAfter(nextValidTime);
        long interval = subsequentNextValidTime.getTime() - nextValidTime.getTime();
        return new Date(nextValidTime.getTime() - interval);
    }

}
