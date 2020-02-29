package com.latticeengines.security.util;

import java.util.Comparator;

import com.latticeengines.domain.exposed.security.Tenant;

class TenantNameSorter implements Comparator<Tenant> {

    public int compare(Tenant oneTenant, Tenant anotherTenant) {
        return oneTenant.getName().compareTo(anotherTenant.getName());
    }

}
