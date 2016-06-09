package com.latticeengines.common.exposed.util;

import static com.cronutils.model.CronType.QUARTZ;

import org.joda.time.DateTime;

import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;

public final class CronUtils {

    private static final CronParser parser = new CronParser(CronDefinitionBuilder.instanceDefinitionFor(QUARTZ));

    public static DateTime getPreviousFireTime(String cron) {
        return getPreviousFireTime(cron, DateTime.now());
    }

    public static DateTime getPreviousFireTime(String cron, DateTime now) {
        ExecutionTime executionTime = ExecutionTime.forCron(parser.parse(cron));
        return executionTime.lastExecution(now);
    }

    public static DateTime getNextFireTime(String cron) {
        return getNextFireTime(cron, DateTime.now());
    }

    public static DateTime getNextFireTime(String cron, DateTime now) {
        ExecutionTime executionTime = ExecutionTime.forCron(parser.parse(cron));
        return executionTime.nextExecution(now);
    }

}
