package com.latticeengines.domain.exposed.propdata.manage;

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
    private static final SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
    private static Calendar calendar = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));

    static {
        formatter.setCalendar(calendar);
    }

    static Date parse(String dateStr) {
        try {
            SimpleDateFormat formatter = new SimpleDateFormat(DATE_FORMAT);
            formatter.setCalendar(calendar);
            return formatter.parse(dateStr);
        } catch (Exception e) {
            log.error("Failed to parse timestamp [" + dateStr + "]", e);
            return  null;
        }
    }

    static String format(Date date) {
        return date == null ? null : formatter.format(date);
    }

}
