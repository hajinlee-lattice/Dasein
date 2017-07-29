package com.latticeengines.domain.exposed.datacloud.manage;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateTimeUtils {

    private static Logger log = LoggerFactory.getLogger(DateTimeUtils.class);
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS z";
    private static final String DATE_TZ_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final String DATE_TX_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
    private static final SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
    private static final SimpleDateFormat formatterTZ = new SimpleDateFormat(DATE_TZ_FORMAT);
    private static final SimpleDateFormat formatterTX = new SimpleDateFormat(DATE_TX_FORMAT);
    private static Calendar calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));

    static {
        formatter.setCalendar(calendar);
        formatterTZ.setCalendar(calendar);
        formatterTX.setCalendar(calendar);
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

    public static Date parseTX(String dateStr) {
        try {
            return formatterTX.parse(dateStr);
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
        return date == null ? null : formatterTX.format(date);
    }

}
