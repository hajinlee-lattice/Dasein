package com.latticeengines.proxy.exposed;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class ProxyUtils {

    public static String shortenCustomerSpace(String customerSpace) {
        return CustomerSpace.parse(customerSpace).getTenantId();
    }

}
