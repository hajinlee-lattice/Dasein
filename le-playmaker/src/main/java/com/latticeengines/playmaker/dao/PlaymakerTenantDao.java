package com.latticeengines.playmaker.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;

public interface PlaymakerTenantDao extends BaseDao<PlaymakerTenant> {

    PlaymakerTenant findByTenantName(String tenantName);

    boolean deleteByTenantName(String tenantName);

    void updateByTenantName(PlaymakerTenant tenant);

}
