package com.latticeengines.common.exposed.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateTimeUtils {

    private static Logger log = LoggerFactory.getLogger(DateTimeUtils.class);
    private static Calendar calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));
    private static Long earliest;
    private static Long latest;
    private static final String UTC = "UTC";
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ssZ";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
    private static final String FILEPATH_DATE_FORMAT_STRING = "yyyy-MM-dd_HH-mm-ss_z";
    private static final SimpleDateFormat filePathDateFormat = new SimpleDateFormat(FILEPATH_DATE_FORMAT_STRING);
    
    public static final String DATE_ONLY_FORMAT_STRING = "yyyy-MM-dd";

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone(UTC));
        filePathDateFormat.setTimeZone(TimeZone.getTimeZone(UTC));
    }

    static {
        calendar.set(1900, Calendar.JANUARY, 1);
        earliest = calendar.getTime().getTime();

        calendar.set(3000, Calendar.JANUARY, 1);
        latest = calendar.getTime().getTime();
    }

    public static Date convertToDateUTCISO8601(String dateString) {
        try {
            return dateFormat.parse(dateString);
        } catch (ParseException e) {
            // HACK.. replace last space char with + char and try again. This is
            // for dealing with URL encoding issue due to randomness in class
            // loading
            dateString = dateString.trim();
            String space = " ";
            String plus = "+";
            if (dateString.contains(space)) {
                dateString = dateString.substring(0, dateString.lastIndexOf(space)) + plus
                        + dateString.substring(dateString.lastIndexOf(space) + 1);
                try {
                    //log.info("Trying to parse updated string: " + dateString);
                    return dateFormat.parse(dateString);
                } catch (ParseException ex) {
                    log.error("Could not parse string even after applying space replacement hack: " + dateString);
                }
            }
            throw new RuntimeException(e);
        }
    }

    public static long convertToLongUTCISO8601(String dateString) {
        return convertToDateUTCISO8601(dateString).getTime();
    }

    public static String convertToStringUTCISO8601(Date date) {
        return dateFormat.format(date);
    }

    public static Boolean isInValidRange(Long timestamp) {
        return timestamp >= earliest && timestamp <= latest;
    }

    public static Boolean isInValidRange(Date dateTime) {
        return isInValidRange(dateTime.getTime());
    }

    public static String toDateOnlyFromMillis(String time) {
        SimpleDateFormat dateOnlyFormat = new SimpleDateFormat(DATE_ONLY_FORMAT_STRING);
        dateOnlyFormat.setTimeZone(TimeZone.getTimeZone(UTC));
        Long timeLong = Long.valueOf(time);
        String result = dateOnlyFormat.format(new Date(timeLong));
        return result;
    }

    public static String currentTimeAsString(Date date) {
        return filePathDateFormat.format(date);
    }

    public static Integer dateToDayPeriod(String dateString) {
        String[] dateUnits = dateString.split("-");
        try {
            int year = Integer.parseInt(dateUnits[0]);
            int month = Integer.parseInt(dateUnits[1]);
            int day = Integer.parseInt(dateUnits[2]);
            int dayPeriod = (year - 1900) * 13 * 32 + month * 32 + day;
            // log.info("Date to day period " + dateString + " " + dayPeriod);
            return new Integer(dayPeriod);
        } catch (Exception e) {
            log.error("Failed to convert " + dateString + " to period", e);
            return null;
        }
    }

    public static String dayPeriodToDate(Integer dayPeriod) {
        if (dayPeriod == null) {
            log.info("Got null period");
            return null;
        }
        int year = dayPeriod / (13 * 32);
        int month = (dayPeriod / 32) % 13;
        if (month == 0) {
            log.error("Invalid month for period " + dayPeriod);
        }
        int day = dayPeriod % 32;
        String monthStr = (month < 10) ? "0" + month : month + "";
        String dayStr = (day < 10) ? "0" + day : day + "";
        String result = (year + 1900) + "-" + monthStr + "-" + dayStr;
        return result;
    }
}
