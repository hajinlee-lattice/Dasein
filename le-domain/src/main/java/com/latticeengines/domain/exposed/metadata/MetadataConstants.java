package com.latticeengines.domain.exposed.metadata;

import static java.time.ZoneOffset.UTC;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class MetadataConstants {

    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_z");

    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone(UTC));
    }

}
