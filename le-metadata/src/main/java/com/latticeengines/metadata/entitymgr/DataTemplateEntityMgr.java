package com.latticeengines.metadata.entitymgr;

import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;

public interface DataTemplateEntityMgr {

    String create(String tenantId, DataTemplate dataTemplate);

    DataTemplate findByUuid(String uuid);

    void updateByUuid(String uuid, DataTemplate dataTemplate);

    void deleteByUuid(String uuid);

}
