package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.dataloader.DLTenantMapping;

public interface DLTenantMappingDao extends BaseDao<DLTenantMapping> {

    DLTenantMapping getDLTenantMapping(String dlTenantId, String dlLoadGroup);
}
