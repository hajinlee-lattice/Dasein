package com.latticeengines.metadata.service;

import java.util.Map;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface DataTemplateService {

    String create(DataTemplate dataTemplate);

    DataTemplate findByUuid(String uuid);

    void updateByUuid(String uuid, DataTemplate dataTemplate);

    void deleteByUuid(String uuid);

    Map<String, ColumnMetadata> getTemplateMetadata(String templateId, BusinessEntity entity);

}
