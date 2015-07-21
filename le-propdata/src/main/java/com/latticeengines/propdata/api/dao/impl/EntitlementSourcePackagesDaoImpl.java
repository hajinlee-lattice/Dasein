package com.latticeengines.propdata.api.dao.impl;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackages;
import com.latticeengines.propdata.api.dao.EntitlementSourcePackagesDao;

public class EntitlementSourcePackagesDaoImpl extends
        BaseDaoImpl<EntitlementSourcePackages> implements
        EntitlementSourcePackagesDao {

    public EntitlementSourcePackagesDaoImpl() {
        super();
    }

    @Override
    protected Class<EntitlementSourcePackages> getEntityClass() {
        return EntitlementSourcePackages.class;
    }

}
