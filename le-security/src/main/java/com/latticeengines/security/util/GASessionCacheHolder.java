package com.latticeengines.security.util;

import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;
import com.latticeengines.security.exposed.globalauth.impl.GlobalSessionManagementServiceImpl;

public class GASessionCacheHolder {
    private static GASessionCache cache;
    private static final int CACHE_TIMEOUT_IN_SEC = (int) ((GlobalSessionManagementServiceImpl.TicketInactivityTimeoutInMinute * 60) * 0.01);

    public static GASessionCache getCache(GlobalSessionManagementService globalSessionMgr) {
        if (cache == null) {
            synchronized (GASessionCacheHolder.class) {
                if (cache == null) {
                    cache = new GASessionCache(globalSessionMgr, CACHE_TIMEOUT_IN_SEC);
                }
            }
        }

        return cache;
    }
}
