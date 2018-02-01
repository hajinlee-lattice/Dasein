package com.latticeengines.datacloud.core.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.CustomerSourceAttribute;

public interface CustomerSourceAttributeDao extends BaseDao<CustomerSourceAttribute> {
    List<CustomerSourceAttribute> getAttributes(String sourceName, String stage, String transformer,
            String dataCloudVersion);
}
