package com.latticeengines.metadata.service;

import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;

public interface DataTemplateService {

    String create(DataTemplate dataTemplate);

    DataTemplate findByUuid(String uuid);

    void updateByUuid(String uuid, DataTemplate dataTemplate);

    void deleteByUuid(String uuid);

}
