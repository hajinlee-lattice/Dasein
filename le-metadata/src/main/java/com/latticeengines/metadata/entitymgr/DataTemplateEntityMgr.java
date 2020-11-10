package com.latticeengines.metadata.entitymgr;

import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;

public interface DataTemplateEntityMgr {

    String create(String tenantId, DataTemplate dataTemplate);

    DataTemplate findByUuid(String tenantId, String uuid);

    void updateByUuid(String tenantId, String uuid, DataTemplate dataTemplate);

    void deleteByUuid(String tenantId, String uuid);

}
