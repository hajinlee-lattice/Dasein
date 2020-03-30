package com.latticeengines.db.exposed.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;

public final class MultiTenantContext {

    protected MultiTenantContext() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(MultiTenantContext.class);

    private static MultiTenantContextStrategy strategy = new ThreadLocalMultiTenantContextStrategy();

    public static void setStrategy(MultiTenantContextStrategy s) {
        if (ThreadLocalMultiTenantContextStrategy.class.getSimpleName()
                .equalsIgnoreCase(strategy.getClass().getSimpleName())) {
            log.info("Changing MultiTenantContextStrategy to " + s.getClass().getSimpleName());
            strategy = s;
        } else {
            log.warn("Cannot change MultiTenantContextStrategy " + strategy.getClass().getSimpleName() + " to "
                    + s.getClass().getSimpleName());
        }
    }

    public static Tenant getTenant() {
        return strategy.getTenant();
    }

    public static Session getSession() {
        return strategy.getSession();
    }

    public static void setTenant(Tenant tenant) {
        if (tenant != null && tenant.getPid() == null) {
            throw new IllegalArgumentException(
                    "Tenant to be put in MultiTenantContext, must have a PID: " + JsonUtils.serialize(tenant));
        }
        strategy.setTenant(tenant);
    }

    public static void clearTenant() {
        log.info("Clearing MultiTenantContext.");
        strategy.clear();
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

    public static String getShortTenantId() {
        Tenant tenant = getTenant();
        if (tenant == null) {
            return null;
        }
        CustomerSpace space = CustomerSpace.parse(tenant.getId());
        return space.getTenantId();
    }

    public static String getPLSTenantId() {
        Tenant tenant = getTenant();
        if (tenant == null) {
            return null;
        }
        return tenant.getId();
    }

    public static String getEmailAddress() {
        Session session = getSession();
        if (session == null) {
            return null;
        }
        return session.getEmailAddress();
    }

    public static User getUser() {
        return strategy.getUser();
    }

}
