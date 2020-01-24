package com.latticeengines.common.exposed.util;

import org.hibernate.Hibernate;
import org.hibernate.proxy.HibernateProxy;

public final class HibernateUtils {

    protected HibernateUtils() {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public static <T> T inflateDetails(T proxy) {
        if (proxy == null) {
            return proxy;
        }
        Hibernate.initialize(proxy);
        if (proxy instanceof HibernateProxy) {
            proxy = (T) ((HibernateProxy) proxy).getHibernateLazyInitializer().getImplementation();
        }
        return proxy;
    }

}
