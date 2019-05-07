package com.latticeengines.datacloud.core.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;

public interface SourceAttributeDao extends BaseDao<SourceAttribute> {

    List<SourceAttribute> getAttributes(String sourceName, String stage, String transformer);

    List<SourceAttribute> getAttributes(String sourceName, String stage, String transformer, String dataCloudVersion);

    String getDataCloudVersionAttrs(String sourceName, String stage,
            String transformer);
}
