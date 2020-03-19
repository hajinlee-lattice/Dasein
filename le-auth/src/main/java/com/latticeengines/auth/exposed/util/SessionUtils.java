package com.latticeengines.auth.exposed.util;

public final class SessionUtils {

    protected SessionUtils() {
        throw new UnsupportedOperationException();
    }

    // 8 hours heart beat timeout
    public static final int TicketInactivityTimeoutInMinute = 480;

    // 24 hours absolute timeout
    public static final int TicketTimeoutInMinute = 1440;

}
