package com.latticeengines.common.exposed.util;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class DateTimeUtils {

    private static Calendar calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));
    private static Long earliest;
    private static Long latest;

    static {
        calendar.set(1900, Calendar.JANUARY, 1);
        earliest = calendar.getTime().getTime();

        calendar.set(3000, Calendar.JANUARY, 1);
        latest = calendar.getTime().getTime();
    }

    public static Boolean isInValidRange(Long timestamp) {
        return timestamp >= earliest && timestamp <= latest;
    }

    public static Boolean isInValidRange(Date dateTime) {
        return isInValidRange(dateTime.getTime());
    }

}
