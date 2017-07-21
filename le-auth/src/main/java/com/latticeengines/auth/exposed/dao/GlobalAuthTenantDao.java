package com.latticeengines.auth.exposed.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;

public interface GlobalAuthTenantDao extends BaseDao<GlobalAuthTenant> {

    List<GlobalAuthTenant> findTenantNotInTenantRight(GlobalAuthUser user);

}
