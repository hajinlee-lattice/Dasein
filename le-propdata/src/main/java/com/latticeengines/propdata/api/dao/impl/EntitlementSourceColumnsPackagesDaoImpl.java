package com.latticeengines.propdata.api.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackages;
import com.latticeengines.propdata.api.dao.EntitlementSourceColumnsPackagesDao;

public class EntitlementSourceColumnsPackagesDaoImpl extends
        BaseDaoImpl<EntitlementSourceColumnsPackages> implements
        EntitlementSourceColumnsPackagesDao {

    @Override
    protected Class<EntitlementSourceColumnsPackages> getEntityClass() {
        return EntitlementSourceColumnsPackages.class;
    }

    public EntitlementSourceColumnsPackagesDaoImpl() {
        super();
    }

}
