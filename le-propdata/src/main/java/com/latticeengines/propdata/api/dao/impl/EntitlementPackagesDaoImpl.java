package com.latticeengines.propdata.api.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.EntitlementPackages;
import com.latticeengines.propdata.api.dao.EntitlementPackagesDao;

public class EntitlementPackagesDaoImpl extends BaseDaoImpl<EntitlementPackages> implements
        EntitlementPackagesDao {

    public EntitlementPackagesDaoImpl() {
        super();
    }

    @Override
    protected Class<EntitlementPackages> getEntityClass() {
        return EntitlementPackages.class;
    }

}
