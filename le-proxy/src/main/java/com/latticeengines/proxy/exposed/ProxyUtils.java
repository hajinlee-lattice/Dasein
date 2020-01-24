package com.latticeengines.proxy.exposed;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public final class ProxyUtils {

    protected ProxyUtils() {
        throw new UnsupportedOperationException();
    }

    public static String shortenCustomerSpace(String customerSpace) {
        return CustomerSpace.parse(customerSpace).getTenantId();
    }

}
