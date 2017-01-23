package com.latticeengines.security.exposed.util;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;

public class MultiTenantContext {
    private static MultiTenantContextStrategy strategy = new GlobalAuthMultiTenantContextStrategy();

    public static void setStrategy(MultiTenantContextStrategy s) {
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
