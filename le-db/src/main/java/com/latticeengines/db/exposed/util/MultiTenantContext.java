package com.latticeengines.db.exposed.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;

public final class MultiTenantContext {

    private static final Logger log = LoggerFactory.getLogger(MultiTenantContext.class);

    private static MultiTenantContextStrategy strategy = new ThreadLocalMultiTenantContextStrategy();

    public static void setStrategy(MultiTenantContextStrategy s) {
        log.info("Changing MultiTenantContextStrategy to " + s.getClass().getSimpleName());
        strategy = s;
    }

    public static Tenant getTenant() {
        return strategy.getTenant();
    }

    public static Session getSession() {
        return strategy.getSession();
    }

    public static void setTenant(Tenant tenant) {
        strategy.setTenant(tenant);
    }

    public static boolean isContextSet() {
        return getTenant() != null;
    }

    public static CustomerSpace getCustomerSpace() {
        Tenant tenant = getTenant();
        if (tenant == null) {
            return null;
        }
        return CustomerSpace.parse(tenant.getId());
    }

    public static String getEmailAddress() {
        Session session = getSession();
        if (session == null) {
            return null;
        }
        return session.getEmailAddress();
    }

}
