package com.latticeengines.common.exposed.util;

import static java.time.ZoneOffset.UTC;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;

public final class NamingUtils {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("_yyyy_MM_dd_HH_mm_ss_z");
    private static final SimpleDateFormat DATE_FORMAT_MILLS = new SimpleDateFormat("_yyyy_MM_dd_HH_mm_ss_SSS_z");

    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone(UTC));
    }

    public static String timestamp(String original) {
        return timestamp(original, new Date());
    }

    public static String timestampWithRandom(String original) {
        return original + DATE_FORMAT_MILLS.format(new Date()) + "_" + new Random().nextInt(100_000);
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

    public static String randomSuffix(String original, int suffixLength) {
        return original + RandomStringUtils.randomAlphanumeric(suffixLength);
    }

    public static String getFormatedDate() {
        return DATE_FORMAT.format(new Date());
    }

}
