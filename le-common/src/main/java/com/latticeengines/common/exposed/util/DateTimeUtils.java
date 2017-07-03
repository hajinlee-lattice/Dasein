package com.latticeengines.common.exposed.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class DateTimeUtils {

    private static Calendar calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));
    private static Long earliest;
    private static Long latest;
    private static final String UTC = "UTC";
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ssZ";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone(UTC));
    }

    static {
        calendar.set(1900, Calendar.JANUARY, 1);
        earliest = calendar.getTime().getTime();

        calendar.set(3000, Calendar.JANUARY, 1);
        latest = calendar.getTime().getTime();
    }

    public static long getTimestampUTCISO8601(String date) {
        try {
            return dateFormat.parse(date).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getDateUTCISO8601(Date date) {
        return dateFormat.format(date);
    }

    public static Boolean isInValidRange(Long timestamp) {
        return timestamp >= earliest && timestamp <= latest;
    }

    public static Boolean isInValidRange(Date dateTime) {
        return isInValidRange(dateTime.getTime());
    }

}
