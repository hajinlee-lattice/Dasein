package com.latticeengines.domain.exposed.util;

import static java.time.ZoneOffset.UTC;

import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.TimeZone;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;

public final class SegmentUtils {

    protected SegmentUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Random random = new Random();

    public static final String dateFormat = "MM/dd/yyyy HH:mm:ss";

    public static String generateDisplayName(String tenantId) {
        StringBuffer val = new StringBuffer();
        val.append(tenantId);
        val.append(" ");
        SimpleDateFormat format = new SimpleDateFormat(dateFormat);
        format.setTimeZone(TimeZone.getTimeZone(UTC));
        val.append(format.format(System.currentTimeMillis()));
        val.append(" - ");
        int length = 5;
        for (int i = 0; i < length; i++) {
            if (random.nextInt(2) % 2 == 0) {
                val.append((char) (random.nextInt(26) + 'A'));
            } else {
                val.append(random.nextInt(10));
            }
        }
        return val.toString();
    }

    public static boolean hasListSegment(MetadataSegment metadataSegment) {
        if (metadataSegment != null && metadataSegment.getListSegment() != null) {
            return true;
        } else {
            return false;
        }
    }

}
