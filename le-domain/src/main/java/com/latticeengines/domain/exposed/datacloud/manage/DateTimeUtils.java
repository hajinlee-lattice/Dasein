package com.latticeengines.domain.exposed.datacloud.manage;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DateTimeUtils {

    private static Log log = LogFactory.getLog(DateTimeUtils.class);
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS z";
    private static final String DATE_TZ_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final String DATE_T_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
    private static final SimpleDateFormat formatterTZ = new SimpleDateFormat(DATE_TZ_FORMAT);
    private static final SimpleDateFormat formatterT = new SimpleDateFormat(DATE_T_FORMAT);
    private static Calendar calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));

    static {
        formatter.setCalendar(calendar);
        formatterTZ.setCalendar(calendar);
        formatterT.setCalendar(calendar);
    }

    public static Date parse(String dateStr) {
        try {
            SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
            formatter.setCalendar(calendar);
            return formatter.parse(dateStr);
        } catch (Exception e) {
            log.error("Failed to parse timestamp [" + dateStr + "]", e);
            return  null;
        }
    }

    public static Date parseTZ(String dateStr) {
        try {
            return formatterTZ.parse(dateStr);
        } catch (Exception e) {
            log.error("Failed to parse timestamp [" + dateStr + "]", e);
            return null;
        }
    }

    public static Date parseT(String dateStr) {
        try {
            return formatterT.parse(dateStr);
        } catch (Exception e) {
            log.error("Failed to parse timestamp [" + dateStr + "]", e);
            return null;
        }
    }

    public static String format(Date date) {
        return date == null ? null : formatter.format(date);
    }

    public static String formatTZ(Date date) {
        return date == null ? null : formatterTZ.format(date);
    }

    public static String formatT(Date date) {
        return date == null ? null : formatterT.format(date);
    }

}
