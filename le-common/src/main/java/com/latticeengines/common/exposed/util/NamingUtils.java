package com.latticeengines.common.exposed.util;

import static java.time.ZoneOffset.UTC;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

public final class NamingUtils {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("_yyyy_MM_dd_HH_mm_ss_z");

    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone(UTC));
    }

    public static String timestamp(String original) {
        return timestamp(original, new Date());
    }

    public static String timestamp(String original, Date date) {
        return original + DATE_FORMAT.format(date);
    }

    public static String uuid(String original) {
        return uuid(original, UUID.randomUUID());
    }

    public static String uuid(String original, UUID uuid) {
        return original + "_" + uuid.toString().replace("-", "_");
    }

}
